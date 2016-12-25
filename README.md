# hadoop-wiki-index
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
A series of programs for generating inverted index of enwiki-multistream.xml and handling queries.

这个仓库存放我分布式系统课程project的代码
分布式系统课程pj主要完成以下工作:

1. 根据[enwiki-latest-pages-articles-multistream.xml](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2)(我用的是20151102版本)构建Wikipedia索引，其中包含TF(Term Frequency)和DF(Document Frequency)信息
2. 在单机处理索引，实现多关键词查询，并将结果按相关度排序
3. 前端页面呈现，处理查询请求并返回结果列表和具体页面

## 依赖
- [wikiclean](https://github.com/lintool/wikiclean) A Java Wikipedia markup to plain text converter

## 使用步骤
1. java程序导出hadoop-wiki-index.jar，注意要添加wikiclean.jar这个依赖
2. Hadoop MapReduce执行以下命令构建索引
```
hadoop jar hadoop-wiki-index.jar xyz.cjr.wiki_index.IDOffsetTitle <path_enwiki.xml> <path_id-offset-title>
hadoop jar hadoop-wiki-index.jar xyz.cjr.wiki_index.PageWordCount <path_enwiki.xml> <path_page-wordcount>
hadoop jar hadoop-wiki-index.jar xyz.cjr.wiki_index.PageMaxWordCount <path_enwiki.xml> <path_page-maxwordcount>
hadoop jar hadoop-wiki-index.jar xyz.cjr.wiki_index.InvertedIndex <path_enwiki.xml> <path_inverted-index>
```
然后依次将输出结果保存到本地`id-offset-title.txt`, `page-wordcount.txt`, `page-maxwordcount.txt`, `inverted-index.txt`，并放在与`cxx/`同级目录。注意最后一个任务执行时间可能超过一个小时。

2. 编译cxx目录下内容
```
make -C cxx
```
会在target目录下生成几个可执行文件，分别是
    - invidxfmt
    - offtable
    - solve

3. 执行下面命令构建索引的索引
```
target/invidxfmt < inverted-index.txt > inverted-index-fmt.txt
target/offtable < inverted-index-fmt.txt > inverted-index-word-offset-df.txt
```

4. 启动solve程序等待请求，solve程序会监听0.0.0.0的23334端口
```
cd target
./solve
```
solve的初始化过程会花费大概2min左右

5. 启动网页后端
```
cd view
npm install
node app.js
```

6. 访问`localhost:3000`就可以进行关键词搜索了。

