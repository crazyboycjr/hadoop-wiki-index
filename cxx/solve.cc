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

const string kEnwikiFile = "enwiki-20151102-pages-articles-multistream.xml";
const string kIDOffsetTitleFile = "id-offset-title.txt";

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
		FILE *fp = fopen(kIDOffsetTitleFile.c_str(), "r");
		ll id;
		int len;
		long offset, pos_in_str;
		char title_buf[kBufSize];
		while (fgets(g_input_buf, kBufSize, fp)) {
			sscanf(g_input_buf, "%lld%ld%[^\n]", &id, &offset, title_buf);

			pos_in_str = title_concat_.length();
			id_offset_[id] = offset;
			ids_.push_back(id);
			pos_.push_back(pos_in_str);

			len = strlen(title_buf);

			for (int i = 0; i < len; i++)
				title_buf[i] = tolower(title_buf[i]);
			title_concat_ += title_buf;

			if (len == 1 && isalpha(title_buf[0]))
				title_index_single_[char_num(title_buf[0])] = pos_in_str;
			else
				title_index_double_[char_num(title_buf[0])][char_num(title_buf[1])].push_back(pos_in_str);
		}
		printf("IDOffsetTitle init finish.\n");
		fclose(fp);
	}

	/*
	 * 接受文章ID，返回文章内容
	 * 即<page></page>之间的内容，包括page标签
	 * 常量时间
	 */
	string ID2Page(ll id)
	{
		string page;
		FILE *fp = fopen(kEnwikiFile.c_str(), "r");
		fseek(fp, id_offset_[id], SEEK_SET);
		char ch, a[7] = {0};
		while ((ch = getchar())) {
			page.push_back(ch);
			for (int i = 0; i < 6; i++)
				a[i] = a[i + 1];
			a[7] = ch;
			if (strcmp(a, "</page>") == 0)
				break;
		}
		fclose(fp);
		return page;
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
			ids.push_back(title_index_single_[word[0] - 'a']);
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
		assert(strlen(needle) >= 2);
		vector<long> &v = title_index_double_[char_num(needle[0])][char_num(needle[1])];
		for (vector<long>::iterator it = v.begin() + lastidx; it != v.end(); ++it) {
			if (strcmp(haystack + *it, needle))
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


string WaitInput(TCPServer &server)
{
	server.accept();
	string msg = server.recv();
	return msg;
}

int main() {
	//read_word_offset();
	//read_title_offset();
	TCPServer server("0.0.0.0", "23334");
	while (1) {
		string input = WaitInput(server);
		cout << "recv:" << input << endl;
		// do sth
	}
	return 0;
}
