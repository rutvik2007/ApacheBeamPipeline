####Program to access bigquery and get the averages

from __future__ import absolute_import
import argparse
import logging
import json
import time
from datetime import datetime
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import pubsub_v1

class avgDecr4Window(beam.DoFn):
    def process(self, e):
        sum = 0
        num=0
        for tup in e[1]:
            if tup[0]<0:
                sum += tup[0]
                num+=1
        if (not num==0):
            avg = sum / num
        else:
            avg = 0
        return_dict={}
        return_dict['thingID']=str(e[0])
        return_dict['time']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return_dict['average']=avg
        return ((return_dict,))

class avg4Window(beam.DoFn):
    def process(self, e):
        sum = 0
        num=0
        for tup in e[1]:        
            sum += tup[0]
        if (not len(e[1])==0):
            avg = sum / len(e[1])
        else:
            avg = 0
        return_dict={}
        return_dict['thingID']=str(e[0])
        return_dict['time']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return_dict['average']=avg
        return ([return_dict])


class printer(beam.DoFn):
    cnt=0
    def process(self,e):
        print(e)
        print(self.cnt)
        self.cnt=self.cnt+1

class converttotuple(beam.DoFn):
    def process(self,e):
        return((e['thingID'],(e['average'],0)),)
        

def run(argv=None):
    global cnt
    cnt=0
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic_read', type=str,help='Pub/Sub topic to read from')
    parser.add_argument('--table1', required=True, help=('Output BigQuery table1 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--table2', required=True, help=('Output BigQuery table2 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--table3', required=True, help=('Output BigQuery table3 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--table4', required=True, help=('Output BigQuery table4 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--table5', required=True, help=('Output BigQuery table3 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--table6', required=True, help=('Output BigQuery table4 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--table7', required=True, help=('Output BigQuery table4 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--table8', required=True, help=('Output BigQuery table4 for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))

    #parser.add_argument('--output')
    args, pipeline_args=parser.parse_known_args(argv)
    options=PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session=True
    options.view_as(StandardOptions).streaming=True
    p=beam.Pipeline(options=options)
    print(args.table1,args.table2,args.table3,args.table4)
    data = (p | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=args.topic_read).with_output_types(bytes)
       | 'Create a tuple with thingID as the key' >> beam.Map(lambda x: ((json.loads(x)['thingID'],(json.loads(x)['volumeDiff'],json.loads(x)['volume']))))
    )
    hourly = (data | 'Window into sliding window of an hour 1 with a new window every 15 minutes' >> beam.WindowInto(window.SlidingWindows(3600,900))
       | 'group by key 1 1' >> beam.GroupByKey()
    )
    hourly_decr = (hourly | 'calculate hourly avg decrease rate' >> beam.ParDo(avgDecr4Window())
    )
    write_hourly_decr_2_bigquery =(hourly_decr| 'write to Hourly_Average_Decrease table' >> beam.io.WriteToBigQuery(args.table1,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    daily_decr=(hourly_decr|'convert to tuple to agg based on key for daily decr' >> beam.ParDo(converttotuple())
        | 'window into sliding windows of 24 hours with a new window every 3 hours 1' >> beam.WindowInto(window.SlidingWindows(3600*24, 3600*3))
        | 'group by key for daily decr' >> beam.GroupByKey()
        | 'calculate  daily decr change rate' >> beam.ParDo(avgDecr4Window())
    )

    write_daily_decr_2_bigquery=(daily_decr | 'write to Daily_Average_Decrease table' >> beam.io.WriteToBigQuery(args.table3,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    hourly_avg = (hourly | 'calculate hourly avg change rate' >> beam.ParDo(avg4Window())
    )   
    write_hourly_avg_2_bigquery = (hourly_avg
       | 'write to Hourly_Averages table' >> beam.io.WriteToBigQuery(args.table2,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    daily_avg = (hourly_avg|'convert to tuple for agg based on key for daily avg'>> beam.ParDo(converttotuple())
        | 'window into sliding windows of 24 hours with a new window every 3 hours 2' >>beam.WindowInto(window.SlidingWindows(3600*24, 3600*3))
        | 'group by key for daily avg' >> beam.GroupByKey()
        | 'calculate daily avg change rate' >> beam.ParDo(avg4Window())
    )
    write_daily_avg_2_bigquery=(daily_avg| 'write to Daily_Averages table' >> beam.io.WriteToBigQuery(args.table4,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    monthly_avg=(daily_avg|'convert to tuple to agg based on key for monthly avg' >> beam.ParDo(converttotuple())
        | 'window into sliding windows of 30 days with a new window every 7.5 days 2' >> beam.WindowInto(window.SlidingWindows(3600*24*30, 3600*24*7.5))
        | 'group by key for monthly avg' >> beam.GroupByKey()
        | 'calculate monthly avg change rate' >> beam.ParDo(avg4Window())
    )
    write_monthly_avg_2_bigquery =(monthly_avg |'Write to Monthly Averages table' >> beam.io.WriteToBigQuery(args.table6,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    yearly_avg=(monthly_avg|'convert to tuple to agg based on key for yearly avg' >> beam.ParDo(converttotuple())
        | 'window into sliding windows of a year with a new window every 30 days 2' >> beam.WindowInto(window.SlidingWindows(3600*365*24, 3600*24*30))
        | 'group by key for yearly avg' >> beam.GroupByKey()
        | 'calculate yearly avg change rate' >> beam.ParDo(avg4Window())
    )
    write_yearly_avg_2_bigquery =(yearly_avg |'Write to yearly Average table' >> beam.io.WriteToBigQuery(args.table8,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    monthly_decr=(daily_decr|'convert to tuple to agg based on key for monthly decr' >> beam.ParDo(converttotuple())
        | 'window into sliding windows of 30 days with a new window every 7.5 days 1' >> beam.WindowInto(window.SlidingWindows(3600*24*30, 3600*24*7.5))
        | 'group by key for monthly decr' >> beam.GroupByKey()
        | 'calculate monthly decr change rate' >> beam.ParDo(avgDecr4Window())
    )
    write_monthly_decr_2_bigquery =(monthly_decr |'Write to Monthly Average Decrease table' >> beam.io.WriteToBigQuery(args.table5,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    yearly_decr=(monthly_decr|'convert to tuple to agg based on key for yearly decr' >> beam.ParDo(converttotuple())
        | 'window into sliding windows of a year with a new window every 30 days 1' >> beam.WindowInto(window.SlidingWindows(3600*365*24, 3600*24*30))
        | 'group by key for yaerly decr' >> beam.GroupByKey()
        | 'calculate yearly decr change rate' >> beam.ParDo(avgDecr4Window())
    )
    write_yearly_decr_2_bigquery =(yearly_decr |'Write to yearly Average Decrease table' >> beam.io.WriteToBigQuery(args.table7,schema=' thingID: STRING, time: DATETIME, average: FLOAT',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
 
    result = p.run()
    result.wait_until_finish()   #for streaming pipeline



if __name__=='__main__':
    logging.getLogger().setLevel(logging.INFO)
    #while not datetime.now().minute==46:
    #    time.sleep(55)
    global cnt
    cnt=0
    run()
