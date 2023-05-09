import os
from sources.db_actions import DB_reqs
from sources.messages import MESSAGES
from sources.job_dict import JOB_DICT

import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types.inline_keyboard import InlineKeyboardMarkup, InlineKeyboardButton

from apscheduler.schedulers.asyncio import AsyncIOScheduler
#
# ..:: SETTING BOT ::..
#
# add scheduler for sending updates
scheduler = AsyncIOScheduler()

# get bots token
TOKEN = os.getenv('TOKEN')
logging.basicConfig(level=logging.INFO)

# init the bot
bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

# create list of jobs for catching buttons
job_list = []
for key in JOB_DICT:
    for job in JOB_DICT[key]:
        job_list.append(job)

# init db actions
db = DB_reqs()

# create buttons for area choice
inline_kb = InlineKeyboardMarkup()
for key in JOB_DICT:
    inline_kb.add(InlineKeyboardButton(key, callback_data=key))

#
# ..:: WORKS ::..
#


# start message
@dp.message_handler(commands=['start'])
async def send_welcome(msg: types.Message):
    db.add_user(msg.from_user.id, msg.from_user.first_name)
    await msg.answer(text=MESSAGES['hello'])


# message with buttons after press 'start'
@dp.message_handler(commands=['choice'])
async def process_command_choise(msg: types.Message):
    await msg.reply("Выбери направление", reply_markup=inline_kb)


# handle pressing button
@dp.callback_query_handler(lambda c: c.data in JOB_DICT)
async def process_callback_area(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, f'выбрал {callback_query.data}')
    # generating buttons with job from choosen area
    jobs_kb = InlineKeyboardMarkup()
    for job in JOB_DICT[callback_query.data]:
        jobs_kb.add(InlineKeyboardButton(job, callback_data=job))
    jobs_kb.add(InlineKeyboardButton('Done', callback_data='Done'))
    await bot.send_message(callback_query.from_user.id, f"теперь профессии", reply_markup=jobs_kb)


# add choosen job to database
@dp.callback_query_handler(lambda c: c.data in job_list)
async def process_callback_job(callback_query: types.CallbackQuery):

    # add data to db
    db.add_user_req_links(callback_query.from_user.id,
                          [callback_query.data])

    # send approve to user
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, f'Добавлено {callback_query.data}!')


# Send answer at 'Done' button
@dp.callback_query_handler(lambda c: c.data == 'Done')
async def process_callback_job(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, MESSAGES['done'])


# sending planned messages
async def send_report():
    update = db.generate_report()
    for id in update:
        await bot.send_message(id, 'Новые вакансии:\n' + update[id])

# add task to scheduler
scheduler.add_job(send_report, trigger='cron', hour=9, minute=00)


if __name__ == '__main__':
    scheduler.start()
    executor.start_polling(dp, skip_updates=True)
