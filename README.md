## **iotdb-utils 简介**
工具项目
## **代码结构**
本项目下分为两个子项目，utils-core和utils-tools项目；  
utils-core中包含主要的业务逻辑以及实现方法，可以以jar包方对外提供服务。  
utils-tools是工具类包，功能实现依赖于core项目，提供命令行方式的工具项目。

## **编译以及测试用例**
mvn clean package -DskipTests;  
mvn install -DskipTests;  
mvn install; 父级项目上执行此命令，打包utils-core到本地仓库  
mvn test; 运行单元测试  
mvn verify; 运行集成测试  
**注释**：  
由于core还没有放在远程仓库上，要顺利的编译tools首先需要执行 mvn install -DskipTests  
integration测试需要指定一台iotdbserver，~~配置类为utils-core项目下test包下的org.apache.iotdb.core.script.SessionProperties~~,  
修改为配置文件了，配置文件为test包下resource下的sessionConfig.properties
你可以在这里配置远程或者本地的iotdbserver。  

**2022-5-23 更新**  
jar包已经更新到公司私仓里   

**2022-08-03 更新**  
版本变更为0.13.1  
添加功能：  
- 基于pipeline的导入导出工具
- core包关键类 ExportStarter、ImportStarter
- tools包中添加新的命令行工具 export-tool-13.1、import-tool-13.1
- tools包中关键类ExportTool13_1、ImportTool13_1


新的命令行工具使用说明：

-  export-tool-13.1.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\validate_test\\83 -i root.** -sy true -se true -c gzip  
-  import-tool-13.1.bat -h 127.0.0.1 -p 6667 -u root -pw root -f D:\validate_test\83 -se true -c gzip
- -h iotdb host地址
- -p 端口号
- -u 用户名
- -pw 密码
- -f fileFolder 导入导出制定文件夹
- -sy 是否需要时间序列结构
- -se 文件生成策略
- -c 压缩格式  目前支持SQL、CSV、SNAPPY、GZIP、LZ4
- -w 筛选条件
- -i iotdb中要导出数据的路径