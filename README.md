# football-data-analysis

Install the requirements if running Python scripts locally without Docker: `pip install -r python/requirements.txt`

Set the configuration defines in a .env file. You can copy the fields to fill from the .env.example file.

In Node-Red, install `@flowfuse/node-red-dashboard` and `node-red-contrib-kafkajs libraries`.

Launch the full stack with 3 parallel consumers:
```bash
docker compose up -d --build --scale consumer=3
```