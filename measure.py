from Metric.RawDataReader.RawDataReader import RawDataReader
from Metric.RawDataReader.DatabaseRawDataReader import DatabaseRawDataReader


from Metric.Query.QueryContext import QueryContext
from Metric.Query.RandomQueryStrategy import RandomQueryStrategy
from Metric.Query.HeadQueryStrategy import HeadQueryStrategy

from Metric.Measure.MeasureContext import MeasureContext
from Metric.Measure.TypesenseRankMeasure import TypesenseRankMeasure
from Metric.Measure.CrawlerAllMetricMeasure import CrawlerAllMetricMeasure
from Metric.Measure.SearchEngineAllMetricMeasure import SearchEngineAllMetricMeasure
from Metric.Measure.CrawlerStatusMeasure import CrawlerStatusMeasure

from Database.Database import Database
from Database.CrawlerModels import Base as CrawlerBase
from Database.MetricModels import Base as MetricBase
from Database.ModelFactory.AppModelFactory import AppModelFactory
from Database.utils import createAllMetricModel, createDB
from Metric.Query.SerpApi import require_searches

from argparse import ArgumentParser

import os

from sqlalchemy import select, desc

def get_latest_batch_id(db, modelFactory) -> int:
    """
    獲取最新 MetricBatch 的 ID。
    回傳: int (若表為空則回傳 None)
    Skips empty batches, the same rule DatabaseRawDataReader uses to pick the batch it reuses.
    """
    MetricBatch = modelFactory.create_metric_batches()
    
    with db.session() as session:
        # 1. 只選取 id 欄位 (Scalar Select) -> 節省記憶體與傳輸頻寬
        # 2. 根據 id 倒序排列 (利用 PK Index) -> 極速
        # 3. 取第一筆
        stmt = select(MetricBatch.id).where(MetricBatch.meta_total_queries > 0)\
            .order_by(MetricBatch.id.desc()).limit(1)
        
        # scalar() 會直接回傳數值 (int) 或 None
        latest_id = session.execute(stmt).scalar()
        
        return latest_id

def parseArgs():
    parser = ArgumentParser()

    parser.add_argument("--strategy", nargs='+', choices=['random', 'head'], default=[], help="raw data path")
    parser.add_argument("--crawler_db_url", help="crawler url")
    parser.add_argument("--metric_db_url", help="metric url")
    parser.add_argument("--select_db_url", help="selectdb url (for index coverage)")

    parser.add_argument("--create", action='store_true', help="create dataset")
    parser.add_argument("--rawdatareader", choices=['db'], default='db', help="raw data reader strategy")
    parser.add_argument("--update", type=int, default=14, help="auto generator days")
    parser.add_argument("--keywordNums", type=int, default=100, help="Metric Data Keyword Nums")

    parser.add_argument("--test", action='store_true', help="test performance")
    parser.add_argument("--typesense_url", help="typesense url")
    parser.add_argument("--measure", nargs='+', choices=['status', 'rank', 'crawler_all', 'all'], help="raw data path")

    parser.add_argument("--createtable", action='store_true', help="create table")

    args = parser.parse_args()
    return args


def createDataset(args, modelFactory: AppModelFactory, crawlerDB, metricDB):
    rawDataReader: RawDataReader = None
    if args.rawdatareader == "db":
        rawDataReader = DatabaseRawDataReader(metricDB, modelFactory, args.update)
    # Check the key and quota first: a bad key must stop the run before any batch is written.
    # Up to one search per golden keyword, plus one trending request per geo.
    require_searches(args.keywordNums * len(args.strategy) + len(rawDataReader.countries))
    rawData = rawDataReader.readData()
    batch_id = get_latest_batch_id(metricDB, modelFactory)

    context: QueryContext = QueryContext()

    if 'random' in args.strategy:
        context.setQueryStrategy(RandomQueryStrategy(metricDB, modelFactory, batch_id, rawData, args.keywordNums))
        context.getGoldenSet()
    if 'head' in args.strategy:
        context.setQueryStrategy(HeadQueryStrategy(metricDB, modelFactory, batch_id, rawData, args.keywordNums))
        context.getGoldenSet()

def test(args, modelFactory: AppModelFactory, crawlerDB, metricDB, selectDB):
    context: MeasureContext = MeasureContext()

    if 'status' in args.measure:
        context.setMeasure(CrawlerStatusMeasure(modelFactory, crawlerDB, metricDB))
        context.test()

    for tag in args.strategy:
        if 'crawler_all' in args.measure:
            context.setMeasure(CrawlerAllMetricMeasure(modelFactory, crawlerDB, metricDB, selectDB, get_latest_batch_id(metricDB, modelFactory), tag))
            context.test()

def main():
    args = parseArgs()

    modelFactory = AppModelFactory(CrawlerBase, MetricBase)
    
    if args.createtable:
        createAllMetricModel(modelFactory)
    
    crawlerDB = createDB("crawler", "crawler", args.crawler_db_url, "crawlerdb")
    metricDB = createDB("metric", "metric", args.metric_db_url, "metricdb", args.createtable, MetricBase)
    selectDB = createDB("select", "select", args.select_db_url, "selectdb") if args.select_db_url else None

    if args.create:
        createDataset(args, modelFactory, crawlerDB, metricDB)
    if args.test:
        test(args, modelFactory, crawlerDB, metricDB, selectDB)

if __name__ == '__main__':
    main()
