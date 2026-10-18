//! Latest-only controller observations. Reports create no SQL history or receipts.
use super::{database::*, Controller};
use crate::{collaboration::{Query,QueryResult},machine_resources::*,protocol::*};
use rusqlite::OptionalExtension;
use std::{collections::BTreeMap,sync::Arc,time::Instant};

#[derive(Clone)]
pub struct Config {
    pub profile: Arc<Profile>,
    /// Controller-local monotonic time, independently injectable from wall time.
    pub monotonic_ms: Arc<dyn Fn()->u64+Send+Sync>,
}
impl Config {
    pub fn new(profile:Profile)->std::result::Result<Self,String> {
        profile.validate()?;
        if profile.local().role!=Role::Controller {return Err("controller host profile must select its controller row".into());}
        let origin=Instant::now();
        Ok(Self {profile:Arc::new(profile),monotonic_ms:Arc::new(move||u64::try_from(origin.elapsed().as_millis()).unwrap_or(u64::MAX))})
    }
}
struct Latest { sample:Sample, bytes:Vec<u8>, received_ms:u64, monotonic_ms:u64 }
#[derive(Default)]
pub(super) struct State { latest:BTreeMap<String,Latest> }
impl State {
    pub(super) fn report(&mut self,tx:&StoreContext<'_>,cfg:&super::ControllerConfig,sample:&Sample)->Result<Data> {
        let selected=cfg.machine_resources.as_ref().ok_or_else(||Fault::Refused("machine_resources_unconfigured","host observation profile is not configured".into()))?;
        let machine=selected.profile.machines.iter().find(|m|m.machine_id==sample.machine_id)
            .ok_or_else(||Fault::Refused("machine_identity","machine is not in the configured mapping".into()))?;
        sample.validate(machine,&selected.profile.bounds).map_err(|code|Fault::Refused(code,"invalid machine resource observation".into()))?;
        if machine.role==Role::Worker && registered_incarnation(tx,machine)?.as_ref()!=sample.worker.as_ref().map(|w|&w.incarnation) {
            return refuse("stale_worker","machine report is not from the current registered worker incarnation");
        }
        let bytes=serde_json::to_vec(sample)?;
        let monotonic_ms=(selected.monotonic_ms)();
        if let Some(old)=self.latest.get(&sample.machine_id).filter(|old| old.sample.worker==sample.worker) {
            let same_epoch=old.sample.producer_epoch==sample.producer_epoch;
            if !same_epoch {
                let (old_boot,old_start)=parse_producer_epoch(&old.sample.producer_epoch).ok_or_else(||Fault::Store("retained observation epoch invalid".into()))?;
                let (boot,start)=parse_producer_epoch(&sample.producer_epoch).ok_or_else(||Fault::Store("validated observation epoch invalid".into()))?;
                if boot!=old_boot || start<=old_start {return refuse("producer_epoch","producer restart must advance within the current boot and registered incarnation");}
            }
            if same_epoch && old.sample.sequence==sample.sequence {
                if old.bytes==bytes {return Ok(Data::MachineResourceReport(ReportDisposition::Duplicate));}
                return refuse("sample_conflict","sample sequence already has different content");
            }
            if same_epoch && sample.sequence<old.sample.sequence {return refuse("older_sample","delayed sample cannot replace the latest observation");}
            if monotonic_ms<old.monotonic_ms {return refuse("observation_clock","controller monotonic clock regressed");}
        }
        self.latest.insert(sample.machine_id.clone(),Latest {sample:sample.clone(),bytes,received_ms:(cfg.clock)(),monotonic_ms});
        Ok(Data::MachineResourceReport(ReportDisposition::Replaced))
    }
    fn view(&self,tx:&StoreContext<'_>,cfg:&super::ControllerConfig,machine:&Machine)->Result<View> {
        let selected=cfg.machine_resources.as_ref().ok_or_else(||Fault::Refused("machine_resources_unconfigured","host observation profile is not configured".into()))?;
        // Registration replacement invalidates an old sample without a read
        // mutating state. A retired incarnation cannot submit another report.
        let current=registered_incarnation(tx,machine)?;
        let latest=self.latest.get(&machine.machine_id).filter(|old|
            machine.role==Role::Controller || old.sample.worker.as_ref().map(|w|&w.incarnation)==current.as_ref());
        let age=latest.and_then(|old|(selected.monotonic_ms)().checked_sub(old.monotonic_ms));
        let freshness=match latest {None=>Freshness::Unobserved,Some(_)=>if age.is_some_and(|n|n<selected.profile.bounds.stale_after_ms){Freshness::Fresh}else{Freshness::Stale}};
        Ok(View {machine:machine.clone(),freshness,received_at_unix_ms:latest.map(|l|l.received_ms),receipt_age_ms:age,
            stale_after_ms:selected.profile.bounds.stale_after_ms,as_of_unix_ms:(cfg.clock)(),sample:latest.map(|l|{
                let mut sample=l.sample.clone();
                // Public epoch equality remains observable without exporting
                // kernel boot/process-start details to models or browsers.
                sample.producer_epoch=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,sample.producer_epoch.as_bytes()).as_ref().iter().map(|b|format!("{b:02x}")).collect();
                sample
            })})
    }
}
fn registered_incarnation(tx:&StoreContext<'_>,machine:&Machine)->Result<Option<String>> {
    let Some(id)=&machine.worker_id else{return Ok(None);};
    let raw:Option<String>=tx.query_row("SELECT body FROM workers WHERE id=?1",[id],|r|r.get(0)).optional()?;
    raw.map(|s| {
        if s.len()>tx.limits.transport.body_bytes {return Err(Fault::Store("registered worker exceeds bound".into()));}
        let worker:Worker=decode(&s)?;
        if worker.id!=*id{return Err(Fault::Store("registered worker identity mismatch".into()));}
        Ok(worker.incarnation)
    }).transpose()
}
pub(super) fn query(tx:&StoreContext<'_>,cfg:&super::ControllerConfig,query:&Query)->Result<QueryResult> {
    let configured=cfg.machine_resources.as_ref();
    match query {
        Query::Machine{machine_id}=>{
            super::collaboration_queries::identity(tx.limits,machine_id)?;
            let machine=configured.and_then(|c|c.profile.machines.iter().find(|m|m.machine_id==*machine_id))
                .ok_or_else(||Fault::Refused("not_found","configured machine not found".into()))?;
            let state=tx.machine_resources.ok_or_else(||Fault::Store("machine observation owner absent".into()))?;
            Ok(QueryResult::Machine(state.view(tx,cfg,machine)?))
        }
        Query::Machines{after,limit}=>{
            super::collaboration_queries::bound(*limit,tx.limits)?;
            let after=super::collaboration_queries::start(tx.limits,after,"machines","")?;
            let mut machines=configured.map(|c|c.profile.machines.iter().collect::<Vec<_>>()).unwrap_or_default();
            machines.sort_by(|a,b|a.machine_id.cmp(&b.machine_id));
            let mut rows=Vec::new();
            for machine in machines.into_iter().filter(|m|m.machine_id>after).take(*limit+1) {
                let visible=machine.role==Role::Controller || registered_incarnation(tx,machine)?.is_some();
                let view=if visible {Some(tx.machine_resources.ok_or_else(||Fault::Store("machine observation owner absent".into()))?.view(tx,cfg,machine)?)}else{None};
                rows.push((machine.machine_id.clone(),view));
            }
            Ok(QueryResult::Machines(super::collaboration_queries::project_page(tx,rows,*limit,"machines","")?))
        }
        _=>refuse("invalid_input","not a machine resource query"),
    }
}
impl Controller {
    pub fn machine_refresh_interval_ms(&self)->Option<u64> {
        self.inner.config.machine_resources.as_ref().map(|c|c.profile.bounds.sample_interval_ms)
    }
    /// Explicit host lifecycle startup. Querying a view never starts a collector.
    pub fn start_machine_resources(&self)->std::result::Result<Option<Producer>,String> {
        let Some(config)=&self.inner.config.machine_resources else{return Ok(None);};
        let epoch=producer_epoch(config.profile.bounds.kernel_read_bytes)?;
        let controller=self.clone();
        Producer::start(config.profile.clone(),None,epoch,move|sample| {
            let controller=controller.clone();
            async move {let _=controller.handle(Request{protocol_revision:WIRE_REVISION,request_id:"controller-machine-observation".into(),command:Operation::ReportMachineResources{sample}}).await;}
        }).map(Some)
    }
}
/// Marker25 protects retained typed reads, although latest samples are ephemeral.
pub(super) fn validate_existing(tx:&StoreContext<'_>,cfg:&super::ControllerConfig,version:i64)->Result<()> {
    if version>=super::application_schema::MACHINE_RESOURCES_VERSION{return Ok(());}
    let bound=cfg.max_receipts.checked_add(cfg.max_cleanup_receipts).ok_or_else(||Fault::Store("machine vocabulary inventory overflow".into()))?;
    let mut statement=tx.prepare("SELECT id FROM receipts ORDER BY id")?;
    let mut rows=statement.query([])?;
    let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>bound{return Err(Fault::Store("machine vocabulary receipt inventory exceeds bound".into()));}
        let id:String=row.get(0)?;
        let Some(record)=super::metadata::scan_inline(tx,&id)? else {continue};
        let selected=match &record.request {
            Operation::ReportMachineResources{..}|Operation::QueryCollaboration{query:Query::Machines{..}|Query::Machine{..}}=>true,
            Operation::CallCollaboration{call}=>matches!(call.tool.as_str(),"collab_machines_list"|"collab_machine_get"),
            _=>false,
        };
        if selected || outcome_requires_telemetry(&record.outcome) {return Err(Fault::Store("machine resource vocabulary requires schema25; original bytes retained".into()));}
    }
    Ok(())
}
