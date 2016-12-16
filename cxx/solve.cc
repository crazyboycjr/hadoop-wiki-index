#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <cctype>

#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>

#include "network.h"

const string kEnwikiFile = "../enwiki-20151102-pages-articles-multistream.xml";
const string kIDOffsetTitleFile = "../id-offset-title.txt";

using namespace std;
typedef long long ll;

#define for_iter(i, n) for (__typeof(n.begin())i = n.begin(); i != n.end(); ++i)

const int kBufSize = 100000;
char g_input_buf[kBufSize];

class IDOffsetTitle {
public:
	IDOffsetTitle(const string &filename)
	{
		Init(filename);
	}

	/*
	 * 从filename中读取<id> <offset> <title>内容
	 * 建立合适的数据结构，以支持下面的查找操作
	 */
	void Init(const string &filename)
	{
		FILE *fp = fopen(filename.c_str(), "r");
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

			for (int i = 0; i < len; i++)
				title_buf[i] = tolower(title_buf[i]);
			title_concat_ += title_buf;

			/*
			 * Warning!
			 * because every title starts with ' ' a space
			 */
			if (len == 2 && isalpha(title_buf[1]))
				title_index_single_[char_num(title_buf[1])] = pos_in_str;
			else
				title_index_double_[char_num(title_buf[1])][char_num(title_buf[2])].push_back(pos_in_str);
		}
		printf("IDOffsetTitle init finish.\n");
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
		FILE *fp = fopen(kEnwikiFile.c_str(), "r");
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
		fclose(fp);
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

	int char_num(int ch) { return ch - 'a'; }

	/* ID到偏移量的hash */
	unordered_map<ll, long> id_offset_;
	/* 所有title字符串的拼接 */
	string title_concat_;
	/* 第i篇文章的id */
	vector<ll> ids_;
	/* 第i篇文章title在字符串中的下标*/
	vector<long> pos_;
	/* 索引两个字母开头的所有位置 加速查找 */
	vector<long> title_index_double_[26][26];
	long title_index_single_[26];
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
	IDOffsetTitle query_class = IDOffsetTitle(kIDOffsetTitleFile);
	TCPServer server("0.0.0.0", "23334");
	while (1) {
		string input = WaitInput(server);
		cout << "recv:" << input << endl;
		// do sth
		vector<ll> ids = query_class.Word2ID(input);
		if (ids.size() == 0) {
			puts("No page found.");
			server.send("No page found.");
		} else {
			puts("Found some pages:");
			for_iter(id, ids)
				printf("%lld\n", *id);
			for_iter(id, ids) {
				server.send(query_class.ID2Page(*id) + "\r\n\r\n");
			}
		}
	}
	return 0;
}
