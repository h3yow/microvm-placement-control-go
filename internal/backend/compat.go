package backend

import "strings"

// VersionCompatible 判定版本兼容：
// required 以 ".*" 结尾时仅要求主版本号一致（如 "1.*" 兼容 "1.7.0"），
// 否则要求完全一致。该规则确定、无副作用，用于版本兼容判定。
func VersionCompatible(required, actual string) bool {
	if strings.HasSuffix(required, ".*") {
		wantMajor := strings.TrimSuffix(required, ".*")
		gotMajor := strings.SplitN(actual, ".", 2)[0]
		return wantMajor == gotMajor
	}
	return required == actual
}
