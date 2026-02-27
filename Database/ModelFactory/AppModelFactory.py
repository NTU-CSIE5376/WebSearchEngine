from Database.ModelFactory.DynamicModelFactory import DynamicModelFactory
from Database.MetricModels import CrawlerStatMixin, MetricCoverageMixin, MetricBatch, MetricQuery, MetricURL
from Database.CrawlerModels import UrlStateCurrentMixin, ContentFeatureCurrentMixin, DomainState, DomainStatsDaily, SummaryDaily, UrlLink

class AppModelFactory():
    def __init__(self, crawlerBase, metricBase):
        self.crawlerModelFactory = DynamicModelFactory(crawlerBase)
        self.metricModelFactory = DynamicModelFactory(metricBase)

        self.crawlerBase = crawlerBase
        self.metricBase = metricBase
    
    def getCrawlerBase(self):
        return self.crawlerBase
    
    def getMetricBase(self):
        return self.metricBase

    def create_crawler_stat_model(self, suffix: str):
        return self.metricModelFactory.get_or_create(
            mixin=CrawlerStatMixin,
            table_base_name="crawler_stat",
            class_base_name="CrawlerStat",
            suffix=suffix
        )
    
    def create_metric_coverage_model(self, set_type: str, suffix: str):
        """HeadSet/RandomSet"""
        return self.metricModelFactory.get_or_create(
            mixin=MetricCoverageMixin,
            table_base_name=f"metric_{set_type.lower()}",
            class_base_name=f"Metric{set_type}",
            suffix=suffix
        )
    
    def create_metric_batches(self):
        return MetricBatch
    
    def create_metric_queries(self):
        return MetricQuery
    
    def create_metric_url(self):
        return MetricURL
    
    def create_url_state_current_model(self, idx: int):
        """Dynamically create UrlStateCurrent ORM class for a given shard table."""
        return self.crawlerModelFactory.get_or_create(
            mixin=UrlStateCurrentMixin,
            table_base_name=f'url_state_current',
            class_base_name=f'UrlStateCurrent',
            suffix=f'{idx:03}'
        )

    def create_content_feature_current_model(self, idx: int):
        return self.crawlerModelFactory.get_or_create(
            mixin=ContentFeatureCurrentMixin,
            table_base_name=f'content_feature_current',
            class_base_name=f'ContentFeatureCurrent',
            suffix=f'{idx:03}'
        )

    def create_domain_state_model(self):
        return DomainState

    def create_domain_daily_model(self):
        return DomainStatsDaily
    
    def create_summary_model(self):
        return SummaryDaily
    
    def create_url_link_model(self):
        return UrlLink
