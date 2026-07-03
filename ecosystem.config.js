module.exports = {
  apps: [
    {
      name: "scanner-market-discovery",
      script: "src/market_discovery.py",
      interpreter: "python3",
      cwd: __dirname,
      watch: false,
      autorestart: true,
      max_restarts: 10,
      env: {
        PYTHONPATH: `${__dirname}/src:${__dirname}/config`,
        PAPER_MODE: "true"
      }
    },
    {
      name: "scanner-maintest",
      script: "src/maintest.py",
      interpreter: "python3",
      cwd: __dirname,
      watch: false,
      autorestart: true,
      max_restarts: 10,
      env: {
        PYTHONPATH: `${__dirname}/src:${__dirname}/config`,
        PAPER_MODE: "true"
      }
    },
    {
      name: "scanner-uma-watcher",
      script: "src/uma_oracle_watcher.py",
      interpreter: "python3",
      cwd: __dirname,
      watch: false,
      autorestart: true,
      max_restarts: 20,
      env: {
        PYTHONPATH: `${__dirname}/src:${__dirname}/config`,
        PAPER_MODE: "true"
      }
    },
    {
      name: "scanner-performance-worker",
      script: "src/performance_worker.py",
      interpreter: "python3",
      cwd: __dirname,
      watch: false,
      autorestart: true,
      env: {
        PYTHONPATH: `${__dirname}/src:${__dirname}/config`
      }
    },
    {
      name: "scanner-retro-worker",
      script: "src/retro_worker.py",
      interpreter: "python3",
      cwd: __dirname,
      watch: false,
      autorestart: true,
      env: {
        PYTHONPATH: `${__dirname}/src:${__dirname}/config`
      }
    },
    {
      name: "scanner-enrich-v2",
      script: "src/enrich_v2.py",
      interpreter: "python3",
      cwd: __dirname,
      watch: false,
      autorestart: true,
      max_restarts: 10,
      env: {
        PYTHONPATH: `${__dirname}/src:${__dirname}/config`
      }
    }
  ]
};
