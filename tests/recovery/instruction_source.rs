//! Actual worker, controller, Git and stdio bridge; controlled executor only.
//! The secondary repository is provisioned by the fixture, never acquired by Borg.
use super::*;
use borg_runtime::{
    mcp,
    repository_catalogue::{Registration, Revision},
    worker::process::component,
};

const PEER: &str = r#"#!/usr/bin/python3
import json, os, socket, subprocess, sys
from pathlib import Path
root = Path(__file__).parent
cfg = json.loads(Path(__file__).with_suffix('.json').read_text())
thread, turn = 'instructions-thread', 'instructions-turn'
bridge = None
serial = 0
def send(x): print(json.dumps(x), flush=True)
def rpc(method, params):
    global serial
    serial += 1
    bridge.stdin.write(json.dumps({'jsonrpc':'2.0','id':serial,'method':method,'params':params})+'\n'); bridge.stdin.flush()
    reply = json.loads(bridge.stdout.readline())
    assert reply['id'] == serial and 'error' not in reply, reply
    return reply['result']
for line in sys.stdin:
    q=json.loads(line); method=q.get('method')
    if 'id' not in q: continue
    if method=='initialize': result={}
    elif method=='command/exec':
        assert q['params']['command']==['/usr/bin/true']
        result={'exitCode':0,'stdout':'','stderr':''}
    elif method=='thread/start':
        server=q['params']['config']['mcp_servers']['borg_collaboration']
        host=json.loads(Path(server['args'][2]).read_text())
        assert host['instruction_source']==cfg['accepted_source'], host
        assert not Path('AGENTS.md').exists() and not Path('constitution.md').exists()
        result={'thread':{'id':thread},'model':'gpt-6-astra','reasoningEffort':'max'}
    elif method=='turn/start':
        prompt=q['params']['input'][0]['text']
        assert 'source="instructions"' in prompt and cfg['commit'] in prompt
        assert 'Do not push, publish, open pull requests' in prompt
        send({'id':q['id'],'result':{'turn':{'id':turn,'status':'inProgress'}}})
        bridge=subprocess.Popen([server['command']]+server['args'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(root/'bridge-peer.log').open('a'),text=True)
        rpc('initialize',{'protocolVersion':cfg['mcp_version'],'capabilities':{},'clientInfo':{'name':'controlled-instruction-peer','version':'1'}})
        tools=rpc('tools/list',{})['tools']
        reader=next(t for t in tools if t['name']=='collab_repository_read')
        assert reader['inputSchema']['properties']['source']['enum']==['start','checkpoint','instructions']
        observed=[]
        for i,path in enumerate(['AGENTS.md','constitution.md','spec/core.md']):
            args={'source':'instructions','path':path,'start_line':1,'max_lines':16}
            reply=rpc('tools/call',{'name':'collab_repository_read','arguments':args,'_meta':{'callId':'required-'+str(i),'threadId':thread,'sessionId':'instruction-session'}})
            assert reply['isError'] is False, reply
            result=json.loads(reply['content'][0]['text'])
            assert result['reply']['status']=='ok' and result['reply']['data']['kind']=='repository', result
            value=result['reply']['data']['value']
            assert value['source']['commit']==cfg['commit'] and value['source']['registration']==cfg['binding']
            assert value['untrusted'] is True and value['data']['text']==cfg['files'][path]
            observed.append(result)
        rpc('ping',{})
        (root/'instruction-read-evidence.json').write_text(json.dumps({'bridge_pid':bridge.pid,'host':host,'results':observed,'before_edit':Path('answer.txt').read_text()}))
        with socket.create_connection(('127.0.0.1',cfg['port']),timeout=20) as gate:
            gate.sendall((json.dumps({'pid':os.getpid(),'cwd':os.getcwd(),'before':Path('answer.txt').read_text(),'thread':thread,'turn':turn})+'\n').encode())
            assert gate.recv(1)==b'G'
        Path('answer.txt').write_text('after\n')
        send({'method':'item/completed','params':{'item':{'type':'agentMessage','text':'Read exact committed instruction source, then changed answer.txt'}}})
        send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed'}}})
        continue
    elif method=='turn/interrupt': result={}
    else: result={}
    send({'id':q['id'],'result':result})
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coding_worker_reads_pinned_secondary_instructions_before_primary_edit() {
    let mut lab = Lab::new("secondary-instructions").await;
    restart_instruction_controller(&mut lab).await;
    let source = lab.root.join("spec-source");
    fs::create_dir(&source).unwrap();
    fs::create_dir(source.join("spec")).unwrap();
    let files = json!({"AGENTS.md":"Read constitution.md and spec/core.md from this exact source.\n",
        "constitution.md":"Keep specification source read only.\n", "spec/core.md":"Set answer.txt to after in the task primary checkout.\n"});
    for (name, text) in files.as_object().unwrap() {
        fs::write(source.join(name), text.as_str().unwrap()).unwrap();
    }
    git(&source, &["init", "-q"]);
    git(&source, &["add", "."]);
    git(&source, &["commit", "-qm", "explicit instruction input"]);
    let commit = git(&source, &["rev-parse", "HEAD"]);
    let store = lab.root.join("spec.git");
    git(
        &lab.root,
        &[
            "clone",
            "--bare",
            "--quiet",
            source.to_str().unwrap(),
            store.to_str().unwrap(),
        ],
    );
    let binding = Revision {
        repository_id: "registered-spec".into(),
        registration_revision: "r1".into(),
    };
    let registration = Registration {
        pull_request_target: None,
        repository_id: binding.repository_id.clone(),
        registration_revision: binding.registration_revision.clone(),
        display_name: "Explicit secondary instructions".into(),
        source_endpoint: "https://example.invalid/registered-spec.git".into(),
        checkpoint_endpoint: "https://example.invalid/unused-checkpoints.git".into(),
        source_store_key: "spec.git".into(),
        checkpoint_store_key: "unused-spec-checkpoint.git".into(),
    };
    let registered = lab
        .client
        .call(Operation::RegisterRepository {
            operation_id: "register-instructions".into(),
            registration: registration.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        registered.outcome,
        Outcome::Ok {
            data: Data::RepositoryRegistration(_)
        }
    ));
    let mut input = lab.input("instruction-agent");
    input.instruction_source = Some(RepositoryReadSource {
        registration: Some(binding.clone()),
        repository: String::new(),
        start_commit: commit.clone(),
    });
    let submitted = task(
        lab.client
            .call(Operation::SubmitTask {
                operation_id: "submit-instructions".into(),
                input,
            })
            .await
            .unwrap(),
    );
    let accepted_source = submitted.input.instruction_source.clone().unwrap();
    assert_eq!(accepted_source.repository, registration.source_endpoint);
    assert_eq!(accepted_source.start_commit, commit);
    assert_eq!(
        submitted.input.repository.as_ref().unwrap(),
        &lab.repository
    );
    fs::write(&lab.fixture, PEER).unwrap();
    save(
        lab.fixture.with_extension("json"),
        &json!({"port":lab.gate.local_addr().unwrap().port(),"accepted_source":accepted_source,
        "binding":binding,"commit":commit,"files":files,"mcp_version":mcp::VERSION}),
    );
    let origin = lab.origin.clone();
    lab.start_worker("instruction-worker", "one", &origin);
    let gate = lab.gate().await;
    let evidence: Value =
        serde_json::from_slice(&fs::read(lab.root.join("instruction-read-evidence.json")).unwrap())
            .unwrap();
    assert_eq!(evidence["before_edit"], "before\n");
    assert_eq!(gate.observed["before"], "before\n");
    let active = lab.get(&submitted.id).await;
    let attempt = active.attempt.as_ref().unwrap();
    let state = lab
        .root
        .join("instruction-worker/.borg-worker")
        .join(component("instruction-worker"));
    let bridge_state = state.join("codex").join(component(&attempt.id));
    let bytes = fs::read(bridge_state.join("collaboration-codex/calls.json")).unwrap();
    let journal: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        journal["instruction_source"],
        serde_json::to_value(&accepted_source).unwrap()
    );
    assert_eq!(journal["entries"].as_array().unwrap().len(), 3);
    for (entry, result) in journal["entries"]
        .as_array()
        .unwrap()
        .iter()
        .zip(evidence["results"].as_array().unwrap())
    {
        assert_eq!(entry["delivered"], true);
        assert_eq!(&entry["outcome"], result);
        let invocation: mcp::Invocation =
            serde_json::from_value(entry["invocation"].clone()).unwrap();
        let original = lab
            .client
            .call(Operation::GetCollaborationCall {
                caller: invocation.caller.clone(),
                operation_id: invocation.operation_id.clone(),
            })
            .await
            .unwrap();
        let original = mcp::recovered(&invocation, original).unwrap();
        let delivered: mcp::CallResult = serde_json::from_value(result.clone()).unwrap();
        assert_eq!(original.reply.outcome, delivered.reply.outcome);
        assert_eq!(original.reply.commit, delivered.reply.commit);
        assert_eq!(original.requires_quiescence, delivered.requires_quiescence);
    }
    gate.release().await;
    let final_task = lab.successful(&submitted.id).await;
    assert_eq!(final_task.input, submitted.input);
    assert_eq!(lab.starts.len(), 1);
    assert_eq!(
        git(
            &lab.root,
            &["--git-dir", store.to_str().unwrap(), "rev-parse", "HEAD"]
        ),
        commit
    );
    for (name, text) in files.as_object().unwrap() {
        assert_eq!(
            git(
                &lab.root,
                &[
                    "--git-dir",
                    store.to_str().unwrap(),
                    "show",
                    &format!("{commit}:{name}")
                ]
            ),
            text.as_str().unwrap().trim()
        );
    }
    assert!(!lab.root.join("unused-spec-checkpoint.git").exists());
    assert_eq!(
        fs::read(bridge_state.join("collaboration-codex/calls.json")).unwrap(),
        bytes
    );
    for worker in &mut lab.workers {
        worker.stop();
    }
    restart_instruction_controller(&mut lab).await;
    assert_eq!(lab.get(&submitted.id).await, final_task);
    lab.close();
}

// Select the fixture-provisioned store for both controller incarnations.
async fn restart_instruction_controller(lab: &mut Lab) {
    if let Some(mut old) = lab.controller.take() {
        old.kill();
    }
    lab.serial += 1;
    let (db, address, origin, store_root) = (
        lab.root.join("tasks.sqlite"),
        lab.address.to_string(),
        lab.origin.clone(),
        lab.root.clone(),
    );
    lab.controller = Some(lab.spawn(
        format!("controller-{}", lab.serial),
        &[
            "controller",
            "--db",
            db.to_str().unwrap(),
            "--listen",
            &address,
            "--origin",
            &origin,
            "--repository-store-root",
            store_root.to_str().unwrap(),
        ],
    ));
    let end = Instant::now() + TIMEOUT;
    loop {
        if let Ok(Reply {
            outcome: Outcome::Ok { .. },
            ..
        }) = lab
            .client
            .call(Operation::ListTasks {
                after: None,
                limit: 1,
            })
            .await
        {
            break;
        }
        assert!(
            Instant::now() < end,
            "controller did not start: {}",
            lab.root.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
