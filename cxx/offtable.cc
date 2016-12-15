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
	int off = 0;
	while (1) {
		while (off++, (c = getchar()) != '\n' && c > 0);
		if (c < 0)
			break;
		scanf("%s", s);
		printf("%s %d\n", s, off);
		off += strlen(s);
	}
	return 0;
}
