## **Developer Environment**

Before the **dash** app can function fully, you need two separate processes running: **redis** and **celery**. In production, **redis** has its own Docker container and **celery** runs as a daemon service in the main app container. For development, it's best to create an extra terminal in VS Code, split it into two panes, and manually launch both processes before you launch your Dash app for evaluation.

---

### **redis:** *in-memory key/value store for job queue exchange*
You need a **redis** server to stand-in for the production server on your local machine. Use Docker CE via WSL2 to deploy one with the `./.devtools/docker-compose.yml` file.

`wsl docker compose -f .devtools/docker-compose.yml up` 

---

### **celery:** *job-queue*
You need to launch a **celery** process that is separate from the dashboard in order for background tasks to execute, standing in for the celery service operated by the production container.

`celery -A backend.jobqueue.worker worker --loglevel=info --pool=solo`