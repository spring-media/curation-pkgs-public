module github.com/spring-media/curation-pkgs-public

go 1.18

replace (
	github.com/spring-media/curation-pkgs-public/pkg/cloudwatchmetrics => ./pkg/cloudwatchmetrics
	github.com/spring-media/curation-pkgs-public/pkg/cvsexport => ./pkg/csvexport
)
