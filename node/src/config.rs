use clap::Parser;
use shared::types::NodeAddr;

#[derive(Parser, Debug)]
#[command(name = "kvstore-node")]
pub struct Config {
    // this node's unique ID e.g. "node-a"
    #[arg(long, env = "NODE_ID")]
    pub node_id: String,

    // this node's index in the cluster e.g. 0, 1, 2
    #[arg(long, env = "NODE_INDEX")]
    pub node_index: usize,

    // total number of nodes in the cluster
    #[arg(long, env = "NUM_NODES", default_value = "3")]
    pub num_nodes: usize,

    // port this node listens on
    #[arg(long, env = "PORT", default_value = "7001")]
    pub port: u16,

    // comma separated peer addresses e.g. "node-b:7002,node-c:7003"
    #[arg(long, env = "PEERS", default_value = "")]
    pub peers_raw: String,

    // where to store WAL and snapshot files
    #[arg(long, env = "DATA_DIR", default_value = "./data")]
    pub data_dir: String,

    // how often to gossip in seconds
    #[arg(long, env = "GOSSIP_INTERVAL", default_value = "2")]
    pub gossip_interval: u64,

    #[arg(long, env = "METRICS_PORT", default_value = "9090")]
    pub metrics_port: u16,
}

impl Config {
    // parse peer addresses from "id:host:port,id:host:port" format
    pub fn peers(&self) -> Vec<NodeAddr> {
        if self.peers_raw.is_empty() {
            return vec![];
        }

        self.peers_raw
            .split(',')
            .filter_map(|p| {
                let parts: Vec<&str> = p.trim().split(':').collect();
                if parts.len() == 3 {
                    Some(NodeAddr {
                        id: parts[0].to_string(),
                        host: parts[1].to_string(),
                        port: parts[2].parse().ok()?,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn wal_path(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.data_dir)
            .join(format!("{}.wal", self.node_id))
    }

    pub fn snapshot_path(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.data_dir)
            .join(format!("{}.snapshot", self.node_id))
    }
}