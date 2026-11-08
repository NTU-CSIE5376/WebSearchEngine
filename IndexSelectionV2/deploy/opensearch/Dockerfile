# OpenSearch for the text index: the official 3.9.0 image plus ICU analysis, which
# segments CJK and Indic text by dictionary (docs/TEXT_INDEX.md).
FROM opensearchproject/opensearch:3.9.0@sha256:adfa61f85025d06b4aeb562e7e74fde7e31c437039c93c3862c17e9acebd6c7c
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch analysis-icu
