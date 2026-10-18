//! Admission occurs before the owner transaction upgrades the marker. No row rewrite.
use super::{database::*,ControllerConfig};
use crate::protocol::*;
pub(super) fn validate_existing(tx:&StoreContext<'_>,cfg:&ControllerConfig,version:i64)->Result<()> {
    if version>=super::application_schema::READ_PAGINATION_VERSION{return Ok(());}
    let maximum=cfg.max_receipts.checked_add(cfg.max_cleanup_receipts).ok_or_else(||Fault::Store("pagination inventory overflow".into()))?;
    let mut q=tx.prepare("SELECT id FROM receipts ORDER BY id")?;
    let mut rows=q.query([])?;
    let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>maximum{return Err(Fault::Store("pagination receipt inventory exceeds bound".into()));}
        let id:String=row.get(0)?;
        let record=super::metadata::read(tx,&id)?.ok_or_else(||Fault::Store("pagination receipt disappeared".into()))?;
        if requires_read_pagination(&record.request)||outcome_requires_read_pagination(&record.outcome) {
            return Err(Fault::Store("read pagination vocabulary requires schema26; original bytes retained".into()));
        }
    }
    Ok(())
}
