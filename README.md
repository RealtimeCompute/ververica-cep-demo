# ververica-cep-demo
Demo of Flink CEP with dynamic patterns.

# !!Note!!
The jar of flink-cep that supports dynamic cep in public maven(https://mvnrepository.com/artifact/com.alibaba.ververica/flink-cep/1.15-vvr-6.0.2-api) is an API jar that does not contain full implementation.
Users can use it for writing codes or packaging but the final job jar must be submitted to [Flink managed by Aliyun](https://www.alibabacloud.com/product/realtime-compute) | [阿里云实时计算Flink版](https://www.aliyun.com/product/bigdata/sc) as the dynamic CEP is currently a commercial feature.
