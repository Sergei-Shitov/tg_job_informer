# Instruction

## Quick Start

1. Clone the repository to your machine.
2. Pass your TOKEN to [env](.env) file. (How to create bot and get token see [here]('https://core.telegram.org/bots#how-do-i-create-a-bot'))
3. Go to [dags](./airflow/dags/) folder and set todays date into string in every dag file
\
`'start_date': datetime(yyyy, m, d, 8, 0, 0)`
4. Open console/terminal in the 'tg_job_informer' and run the command:
\
`docker compose up -d`
5. It will create 4 containers (could take about 10 minutes)
6. Find your bot in telegram and send `/start` command and follow instructions in dialog

## Personalize

Before you start application, you probably would like to personalize the applitation

You can change general messages and list of job's request.
\
Go to [messages](./bot_app/app/sources/messages.py) file and put information what you need.
\
For changing requests put your wishes to [job dict](./bot_app/app/sources/job_dict.py) file, read comments in the top and make changes as you wish. **IMPORTANT** thing - searching will work with **ALL** jobs.
