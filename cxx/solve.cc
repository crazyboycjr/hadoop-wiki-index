#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <cctype>

#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <queue>
#include <utility>

#include "network.h"

const string kEnwikiFile = "../enwiki-20151102-pages-articles-multistream.xml";
const string kIDOffsetTitleFile = "../id-offset-title.txt";
const string kInvertedIndexFile = "../inverted-index-fmt.txt";
const string kInvertedIndexWordOffsetFile = "../inverted-index-word-offset-df.txt";
const string kPageWordCountFile = "../page-wordcount.txt";
const string kPageMaxWordCountFile = "../page-max-wordcount.txt";

using namespace std;
typedef long long ll;

#define for_iter(i, n) for (__typeof(n.begin())i = n.begin(); i != n.end(); ++i)

const int kBufSize = 100000;
char g_input_buf[kBufSize];

class IDOffsetTitle {
public:
	IDOffsetTitle(const string &offset_file, const string &context_file)
	{
		Init(offset_file, context_file);
	}

	/*
	 * 从offset_file中读取<id> <offset> <title>内容
	 * 建立合适的数据结构，以支持下面的查找操作
	 */
	void Init(const string &offset_file, const string &context_file)
	{
		context_fp_ = fopen(context_file.c_str(), "r");
		FILE *fp = fopen(offset_file.c_str(), "r");
		ll id;
		int len;
		long offset, pos_in_str;
		char title_buf[kBufSize];
		while (fgets(g_input_buf, kBufSize, fp)) {
			sscanf(g_input_buf, "%lld%ld%[^\n]", &id, &offset, title_buf);

			//printf("%lld %ld %s\n", id, offset, title_buf);
			pos_in_str = title_concat_.length();
			id_offset_[id] = offset;
			ids_.push_back(id);
			pos_.push_back(pos_in_str);

			len = strlen(title_buf);
			/* hadoop reduce will write a '\t' at the end of each kvpair */
			if (title_buf[len - 1] == '\t')
				title_buf[--len] = '\0';

			for (int i = 0; i < len; i++) {
				title_buf[i] = tolower(title_buf[i]);
				if (!isascii(title_buf[i]))
					title_buf[i] = ' ';
			}
			title_concat_ += title_buf;

			/*
			 * Warning!
			 * because every title starts with ' ' a space
			 */
			if (len == 2)
				title_index_single_[char_num(title_buf[1])] = pos_in_str;
			else
				title_index_double_[char_num(title_buf[1])][char_num(title_buf[2])].push_back(pos_in_str);
		}
		printf("IDOffsetTitle init finished.\n");
		fclose(fp);
	}

	/*
	 * 接受enwiki.xml中的偏移量
	 * 返回文章内容
	 */
	string Offset2Page(long offset)
	{
		printf("In function Offset2Page: handle offset = %ld\n", offset);
		string page;
		/* TODO change fp to private member variable */
		FILE *fp = context_fp_;
		fseek(fp, offset, SEEK_SET);
		char ch, a[7] = {0};
		while ((ch = getc(fp))) {
			page.push_back(ch);
			for (int i = 0; i < 6; i++)
				a[i] = a[i + 1];
			a[6] = ch;
			if (strcmp(a, "</page>") == 0)
				break;
		}
		return page;
	}

	/*
	 * 接受文章ID，返回文章内容
	 * 即<page></page>之间的内容，包括page标签
	 * 常量时间
	 */
	string ID2Page(ll id)
	{
		printf("In function ID2Page: handle id = %lld\n", id);
		return Offset2Page(id_offset_[id]);
	}
	
	vector<string> IDs2Pages(const vector<ll> ids)
	{
		/* For compatibility */
		vector<string> pages;
		for_iter(id, ids) {
			pages.push_back(ID2Page(*id));
		}
		return pages;
	}

	/*
	 * 接受一个单词，从Title中查找子串，返回匹配的前K(default=10)条
	 * Since title_concat_.length() can be very large, this function may be slow
	 */
	vector<ll> Word2ID(const string &word, int K = 10)
	{
		long pos = 0;
		vector<ll> ids;
		assert(word.length() > 0);
		if (word.length() == 1) {
			pos = title_index_single_[word[0] - 'a'];
			printf("%ld\n", pos);
			long idx = lower_bound(pos_.begin(), pos_.end(), pos) - pos_.begin();
			ids.push_back(ids_[idx]);
			return ids;
		}
		long tmp = 0;
		while (K-- && (pos = FindSubStringOrDie(title_concat_.c_str(), word.c_str(), tmp))) {
			long idx = lower_bound(pos_.begin(), pos_.end(), pos) - pos_.begin();
			ids.push_back(ids_[idx]);
		}
		return ids;
	}

private:
	/* 不接受长度 < 2的字符串 */
	ll FindSubStringOrDie(const char *haystack, const char *needle, long &lastidx) {
		int len = strlen(needle);
		assert(len >= 2);
		printf("In FindSubStringOrDir needle = %s\n", needle);
		vector<long> &v = title_index_double_[char_num(needle[0])][char_num(needle[1])];
		for (vector<long>::iterator it = v.begin() + lastidx; it != v.end(); ++it) {
			lastidx++;
			//printf("In FindSubStringOrDir it = %ld, string = %s\n", *it, string(haystack + *it + 1, strlen(needle)).c_str());
			if (strncmp(haystack + *it + 1, needle, len) == 0)
				return *it;
		}
		return 0; // or nullptr?
	}

	int char_num(int ch) { return ch; }

	FILE *context_fp_;

	/* ID到偏移量的hash */
	unordered_map<ll, long> id_offset_;
	/* 所有title字符串的拼接 */
	string title_concat_;
	/* 第i篇文章的id */
	vector<ll> ids_;
	/* 第i篇文章title在字符串中的下标*/
	vector<long> pos_;
	/* 索引两个字母开头的所有位置 加速查找 */
	vector<long> title_index_double_[256][256];
	long title_index_single_[256];
};

struct IDTimesTF {
	IDTimesTF() {id = 0; tfproduct = 0; times = 0;}
	IDTimesTF(ll _id, double _tfproduct, int _times) {
		id = _id;
		tfproduct = _tfproduct;
		times = _times;
	}
	ll id;
	double tfproduct;
	int times;
	bool operator < (const IDTimesTF &a) const {
		return times > a.times || (times == a.times && tfproduct > a.tfproduct);
	}
};

class InvertedIndex {
public:
	/* 
	 * 接受两个参数，前一个文件是后一个文件的偏移量索引
	 */
	InvertedIndex(const string &offset_file, const string &context_file)
	{
		Init(offset_file, context_file);
	}

	void Init(const string &offset_file, const string &context_file)
	{
		context_file_ = context_file;
		FILE *fp = fopen(offset_file.c_str(), "r");
		long offset, df;
		char word_buf[kBufSize];
		while (fgets(g_input_buf, kBufSize, fp)) {
			sscanf(g_input_buf, "%s%ld%ld", word_buf, &offset, &df);
			//printf("%s %ld %ld\n", word_buf, offset, df);
			word_offset_[word_buf] = offset;
			df_[word_buf] = df / 2;
		}
		fclose(fp);

		ll id;
		int count;
		fp = fopen(kPageWordCountFile.c_str(), "r");
		while (~fscanf(fp, "%lld%d\n", &id, &count))
			page_words_[id] = count;
		fclose(fp);
		fp = fopen(kPageMaxWordCountFile.c_str(), "r");
		while (~fscanf(fp, "%lld%d\n", &id, &count))
			page_max_words_[id] = count;
		fclose(fp);
		printf("InvertedIndex init finished.\n");
	}


	vector<string> SplitAndSortByDF(string input)
	{
		vector<string> words;
		char *input_cstr = new char[input.length() + 1];
		strcpy(input_cstr, input.c_str());
		/* Warning: strncpy will not be null-terminated if n <= the length of the string */
		//strncpy(input_cstr, input.c_str(), input.length() + 1);
		for (char *ptr = strtok(input_cstr, " "); ptr != NULL; ptr = strtok(NULL, " ")) {
			int len = strlen(ptr);
			for (int i = 0; i < len; i++)
				ptr[i] = tolower(ptr[i]);
			words.push_back(ptr);
		}
		//sort(words.begin(), words.end(), cmp_df);
		/*
		 * Because the input is typed by user, so we can use selection sort here
		 * which is O(n^2)
		 */
		for (int i = 0; i < (int)words.size() - 1; i++)
			for (int j = i + 1; j < (int)words.size(); j++)
				if (!cmp_df(words[i], words[j]))
					swap(words[i], words[j]);
		return words;
	}

	/*
	 * 接受一个字符串输入，该函数将字符串分成单词
	 * 根据DF排序，然后再根据TF查询相关度最高的K个解
	 * 返回结果文章的id
	 */
	vector<ll> Query(string input, int K = 10, int strategy = 1)
	{
		vector<string> words = SplitAndSortByDF(input);
		puts("After sort by DF, the input is:");
		for_iter(word, words)
			printf("%s\n", word->c_str());
		return Words2Pages(words, K);
	}

	/*
	 * 采用一次一单词
	 * DF小的单词在前面
	 */
	vector<ll> Words2Pages(const vector<string> words, int K = 10, int strategy = 1)
	{
		if (K < 100) {
			/* use normal array */
		} else {
			/* Use priority_queue */
		}
		/* 一次性打开多个，减少seek */
		vector<FILE*> fps;
		for_iter(word, words) {
			FILE *fp =  fopen(context_file_.c_str(), "r");
			fseek(fp, word_offset_[*word] + word->length(), SEEK_SET);
			fps.push_back(fp);
		}
		unordered_map<ll, int> id_times;
		unordered_map<ll, double> id_tfproduct;
		id_times.clear();
		id_tfproduct.clear();

		ll id, tf;
		vector<IDTimesTF> vec;
		for (size_t i = 0; i < words.size(); ++i) {
			long df = df_[words[i]];
			printf("For words %s, df = %ld\n", words[i].c_str(), df);
			for (long j = 0; j < df; j++) {
				id = next_ll(fps[i]);
				tf = next_ll(fps[i]);
				if (id_times[id]++ == 0)
					id_tfproduct[id] = 1.0;
				if (strategy)
					id_tfproduct[id] *= 1.0 * tf / page_words_[id];
				else
					id_tfproduct[id] *= 1.0 * tf / page_max_words_[id];
			}
		}
		for_iter(it, id_times) {
			vec.push_back((IDTimesTF){it->first, id_tfproduct[it->first], it->second});
			//vec.push_back(IDTimesTF(it->first, id_tfproduct[it->first], it->second));
		}
		sort(vec.begin(), vec.end());
		vector<ll> ids;
		for (size_t i = 0; i < (size_t)K && i < vec.size(); i++) {
			printf("id=%lld times=%d tfproduct=%f\n", vec[i].id, vec[i].times, vec[i].tfproduct);
			if (page_words_[vec[i].id] < 100)
				continue;
			ids.push_back(vec[i].id);
		}
		for_iter(fp, fps)
			fclose(*fp);
		return ids;
	}

private:
	bool cmp_df(const string &a, const string &b)
	{
		return df_[a] < df_[b];
	}

	/* id是按字典序排的，所以应该调这个函数 */
	string next_id(FILE *fp)
	{
		char s[30];
		fscanf(fp, "%s", s);
		return s;
	}
	/* Only consider positive number */
	ll next_ll(FILE *fp)
	{
		char c;
		while (c = getc(fp), !isdigit(c));
		ll ret = c - 48;
		while (c = getc(fp), isdigit(c))
			ret = ret * 10 + c - 48;
		return ret;
	}

	string context_file_;
	/*
	 * key: word
	 * value: offset
	 */
	unordered_map<string, long> word_offset_;
	/* 
	 * 文档频率 Document Frequency
	 * key: word
	 * value: df
	 */
	unordered_map<string, long> df_;

	/* 
	 * key: id
	 * value: total words of page
	 */
	unordered_map<ll, int> page_words_;

	unordered_map<ll, int> page_max_words_;
};

string Trim(string str) {
	int start = 0, len = str.length();
	for (string::const_iterator it = str.begin(); it != str.end() && isspace(*it); ++it, ++start);
	for (string::const_reverse_iterator it = str.rbegin(); it != str.rend() && isspace(*it); ++it, --len);
	return str.substr(start, len - start);
}

string WaitInput(TCPServer &server)
{
	server.accept();
	string msg = server.recv();
	return Trim(msg);
}

int main() {
	IDOffsetTitle query_class = IDOffsetTitle(kIDOffsetTitleFile, kEnwikiFile);
	InvertedIndex inverted_index_class = InvertedIndex(kInvertedIndexWordOffsetFile, kInvertedIndexFile);
	TCPServer server("0.0.0.0", "23334");
	while (1) {
		string input = WaitInput(server);
		cout << "recv:" << input << endl;

		/* extract limit */
		size_t pos = input.find_first_of(" ");
		assert(pos != string::npos);
		int limit = strtol(input.substr(0, pos).c_str(), NULL, 10);
		input = input.substr(pos + 1, input.length() - pos - 1);
		printf("limit: %d\n", limit);
		printf("query: %s\n", input.c_str());
		int strategy = 0;
		if (limit < 0) {
			limit = -limit;
			strategy = 1;
		}
		
		// do sth
		vector<ll> result_ids;
		vector<string> result_pages;
		vector<ll> ids = query_class.Word2ID(input, limit);
		unordered_map<ll, int> id_found_in_stage2;

		if (ids.size() == 0) {
			puts("Considering title. No page found.");
		} else {
			puts("Considering title. Found some pages:");
			for_iter(id, ids)
				printf("%lld\n", *id);
		}

		puts("Begin to find in pages...");
		vector<ll> ids2 = inverted_index_class.Query(input, limit, strategy);

		if (ids2.size() == 0) {
			puts("No page found in documents");
		} else {
			for_iter(id, ids2) {
				printf("%lld\n", *id);
				result_ids.push_back(*id);
				id_found_in_stage2[*id] = 1;
			}
			for_iter(id, ids) {
				if (!id_found_in_stage2[*id]) {
					result_ids.push_back(*id);
					//result_pages.push_back(query_class.ID2Page(*id));
				}
			}
		}

		for_iter(id, result_ids)
			result_pages.push_back(query_class.ID2Page(*id));

		if (result_pages.size() == 0) {
			puts("Finally found nothing, try some other words.");
			server.send("Finally found nothing, try some other words.");
			server.end();
		} else {
			for_iter(page, result_pages) {
				if (limit-- == 0)
					break;
				server.send((*page + "\r\n\r\n").c_str());
			}
			server.end();
		}
	}
	return 0;
}
