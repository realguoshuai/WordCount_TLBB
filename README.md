# WordCount_TLBB
统计中文小说天龙八部人物热度

## 遇到的问题一:linux上中文乱码问题
  * 解决,先将小说导入到eclipse,编码转换成utf-8
## 遇到的问题二:不能分词,分的是整段整段的
   * 引入 IKAnalyzer的maven依赖 
## 暂未解决,人名提取  按照value值进行降序
  *人名提取使用awk命令,去除小与两个字,大于5个字的文字(已完成排序)
![](https://github.com/realguoshuai/WordCount_TLBB/blob/master/WordCount_tlbb/src/main/resources/ing.png)

