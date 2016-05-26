from caravel import appbuilder, db, models, viz, utils, app, sm, ascii_art, cache
from caravel.viz import viz_types
import json
from datetime import datetime
import logging
from datetime import datetime
import os
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import sqlite3 as lite
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker


def _cron_func(slice_id):
	slc = (
		db.session.query(models.Slice)
		.filter_by(id=slice_id)
		.first()
		)
	print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
	datasource_type = slc.datasource_type
	datasource_id = slc.table_id
	datasource_class = models.SqlaTable \
	if datasource_type == "table" else models.DruidDatasource
	datasources = (
		db.session
		.query(datasource_class)
		.all()
		)
	datasources = sorted(datasources, key=lambda ds: ds.full_name)
	datasource = [ds for ds in datasources if int(datasource_id) == ds.id]
	datasource = datasource[0] if datasource else None

	d = json.loads(slc.params)
	viz_class = viz_types[slc.viz_type]
	sliceView = viz_class(datasource, form_data=d)
	print("========sliceview==========")
	print(dir(sliceView))
	print("========sliceview==========")
	cache_timeout = 1000
	payload = {
	'cache_timeout': cache_timeout,
	'cache_key': sliceView.cache_key,
	'csv_endpoint': sliceView.csv_endpoint,
	'data': sliceView.get_data(),
	'form_data': sliceView.form_data,
	'json_endpoint': sliceView.json_endpoint,
	'query': sliceView.query,
	'standalone_endpoint': sliceView.standalone_endpoint,
	}
	payload['cached_dttm'] = datetime.now().isoformat().split('.')[0]
	logging.info("Caching for the next {} seconds".format(
		cache_timeout))
	print("===============print slice cache_key==========")
	print(sliceView.cache_key)
	cache.set(sliceView.cache_key, payload, timeout=cache_timeout)
	print(sliceView.get_url(json="true", force="false"))
	print("===============print slice url==========")

# class bgApscheduler():
# 	def __init__(self):
# 		job_store = {'default': SQLAlchemyJobStore(url='sqlite:///example.sqlite')}
# 		executors = {'default': ThreadPoolExecutor(20)}
# 		job_defaults = {'max_instances': 5}
# 		self.bg = BackgroundScheduler(jobstores=job_store, executors=executors, job_defaults=job_defaults)
		

# 	def init_scheduler(self):
# 		self.bg.start()

# 	def add_cron_job(self, slice_id):
# 		self.bg.add_job(_cron_func, 'interval', second=30, id=slice_id, args=slice_id)

bg = None
job_store = None
# executors = None
# job_defaults = None
def init_aps():
	global job_store
	job_store = {'default': SQLAlchemyJobStore(url='sqlite:///example.sqlite')}
	executors = {'default': ThreadPoolExecutor(20)}
	job_defaults = {'max_instances': 5}
	global bg
	bg = BackgroundScheduler(jobstores=job_store, executors=executors, job_defaults=job_defaults)
	bg.start()

def add(slice_id):
	global bg
	global job_store
	job = bg.get_job(slice_id, jobstore='default')
	if job:
		bg.reschedule_job(slice_id, trigger='interval', seconds=20)
	else:
		bg.add_job(_cron_func, 'interval', seconds=30, id=slice_id, args=[slice_id])



