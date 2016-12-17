/*
 * This Program preprocess the offset of each line in inverted-index-fmt.txt
 * for query use, also we can calculate DF here.
 */
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>

using namespace std;
typedef long long ll;
const int maxn = 100000;
char s[maxn];

int main() {
	char c;
	long off = 0;
	while (1) {
		long df = 0;
		while (off++, (c = getchar()) != '\n' && c > 0)
			df += c == ' ';
		if (c < 0)
			break;
		if (df)
			printf(" %ld\n", df);
		if (scanf("%s", s) == EOF)
			break;
		printf("%s %ld", s, off);
		off += strlen(s);
	}
	return 0;
}
