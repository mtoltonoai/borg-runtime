//! Actual Bridge journal/transport protocol; the canonical page is controlled data.
use super::*;
use crate::read_page::*;
#[derive(Clone)]
struct PageTransport { page:ReadPage, state:Arc<Mutex<(usize,usize,BTreeMap<String,CallResult>)>> }
impl Transport for PageTransport {
    async fn call(&self,call:&Invocation)->Result<CallResult,String> {
        let mut state=self.state.lock().unwrap();state.0+=1;
        let mut result=result(call,Mode::Good);result.reply.outcome=Outcome::Ok{data:Data::ReadPage(Box::new(self.page.clone()))};
        state.2.insert(call.operation_id.clone(),result);Err("controlled lost original reply".into())
    }
    async fn call_observed(&self,call:&Invocation)->Result<CallResult,mcp::CallFailure>{
        self.call(call).await.map_err(mcp::CallFailure::Transport)
    }
    async fn lookup(&self,call:&Invocation)->Result<Option<CallResult>,String>{let mut state=self.state.lock().unwrap();state.1+=1;Ok(state.2.get(&call.operation_id).cloned())}
}
fn selected_page()->ReadPage {
    let text="quoted \"\\\n🙂e\u{301} ".repeat(1000);
    let page=ReadPage{subject:Selection::Task{task_id:"task-1".into(),session_id:Some("session-1".into()),part:TaskPart::Instruction},identity:Identity{source:Source::Task{task_id:"task-1".into(),session_id:"session-1".into(),part:TaskPart::Instruction,revision:"session-1".into()},digest:digest(text.as_bytes())},summary:Summary::Task{status:TaskStatus::Active,parts:vec![TaskPart::Instruction]},encoding:Encoding::Utf8,offset:0,returned_bytes:0,total_bytes:text.len(),content:String::new(),complete:false,next_cursor:None,retained_truncation:None};
    fit(page,&text,Some(4096),1024,|p|if serde_json::to_vec(p).unwrap().len()<=4096{Ok(())}else{Err(Error::bound("requested max_bytes"))}).unwrap()
}
#[tokio::test]
async fn shared_read_pagination_codex_bridge_same_page_and_recovery() {
    // Missing config is clean only when the original journal is absent. A
    // dangling link is an existing retained object, never evidence of no calls.
    for object in ["file","directory","dangling-link"] {
        let (_area,cfg,_,_)=setup(Mode::Good);
        admit_read_pagination_from_retained_config(cfg.clone()).unwrap();
        process::private_directory(&cfg.directory()).unwrap();
        let path=cfg.directory().join("calls.json");
        match object {
            "file"=>std::fs::write(&path,b"retained original").unwrap(),
            "directory"=>std::fs::create_dir(&path).unwrap(),
            _=>std::os::unix::fs::symlink("absent-original",&path).unwrap(),
        }
        let original=std::fs::symlink_metadata(&path).unwrap();
        assert_eq!(admit_read_pagination_from_retained_config(cfg).unwrap_err(),"Coding journal lacks its original config");
        let retained=std::fs::symlink_metadata(&path).unwrap();
        assert_eq!(retained.file_type(),original.file_type());assert_eq!(retained.len(),original.len());
    }
    let (_area,cfg,_unused,cancel)=setup(Mode::Good);
    let page=selected_page();assert!(!page.complete);
    let transport=PageTransport{page:page.clone(),state:Arc::new(Mutex::new((0,0,BTreeMap::new())))};
    let mut bridge=Bridge::open(cfg.clone(),transport.clone(),vec![],cancel.clone()).unwrap();
    let arguments=json!({"task_id":"task-1","part":"instruction","max_bytes":4096});
    // Lose the original response, recover that occurrence once, then restart
    // before delivery. Neither recovery fetches the content cursor.
    let (original,_) = bridge.invoke("collab_task_get",arguments.clone(),&meta("page-original")).await.unwrap();
    drop(bridge);
    let mut bridge=Bridge::open(cfg.clone(),transport.clone(),vec![],cancel).unwrap();
    let (wire,index)=bridge.invoke("collab_task_get",arguments.clone(),&meta("page-original")).await.unwrap();
    let result:CallResult=serde_json::from_str(wire["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(result.reply.outcome,Outcome::Ok{data:Data::ReadPage(Box::new(page.clone()))});
    assert_eq!(wire,original);
    let CallBody::Inline(retained) = bridge.journal.entries[index].outcome.as_ref().unwrap() else {
        panic!("expected the original inline page outcome before delivery");
    };
    assert_eq!(retained.reply.outcome,result.reply.outcome);
    bridge.delivered(index).unwrap();let original=std::fs::read(&bridge.path).unwrap();
    let (replay,_)=bridge.invoke("collab_task_get",arguments,&meta("page-original")).await.unwrap();assert_eq!(replay,wire);
    assert_eq!(transport.state.lock().unwrap().0,1);assert_eq!(transport.state.lock().unwrap().1,1);
    assert_eq!(bridge.journal.entries.len(),1);assert_eq!(std::fs::read(&bridge.path).unwrap(),original);
    assert!(admit_read_pagination(&cfg).unwrap_err().contains("worker20"));
    assert_eq!(std::fs::read(&bridge.path).unwrap(),original);
}
