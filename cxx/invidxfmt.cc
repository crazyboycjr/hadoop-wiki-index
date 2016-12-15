#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>

using namespace std;
typedef long long ll;
const int maxn = 100000;
char s[maxn], s2[maxn];
string last, cur;
ll id, cnt;

int main() {
	while (fgets(s, maxn, stdin)) {
		sscanf(s, "%s%lld%lld", s2, &id, &cnt);
		cur = s2;
		if (last == cur) {
			printf(" %lld %lld", id, cnt);
		} else {
			printf("\n%s %lld %lld", s2, id, cnt);
		}
		last = cur;
	}
	puts("");
	return 0;
}
