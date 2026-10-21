---
"@agent-wechat/agent-server": major
---

Consolidate deployment into one host-network `docker-compose.yml` and remove `docker-compose.host.yml`. The configuration requires an existing verified local image, external data volumes, and the matching token file; it no longer publishes bridge-network ports or pulls an image automatically. Update deployment instructions and local configuration file references when adopting this template. Existing running containers and user configuration files are not migrated automatically.
