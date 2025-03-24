/*
敏感词查找,验证,过滤和替换
来源:
https://github.com/importcjj/sensitive
复制dict目录下的文件放在项目目录,调用InitSensitive传入文件路径初始化敏感词
*/
package sensitive

var Filters *Filter

// 初始化敏感词并可选配置敏感词文件
func InitSensitive(dictPath ...string) {
	Filters = New()
	if len(dictPath) > 0 {
		Filters.LoadWordDict(dictPath[0])
	}
}
