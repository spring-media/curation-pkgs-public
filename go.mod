module github.com/spring-media/curation-pkgs-public

go 1.18

require (
	github.com/spring-media/curation-pkgs-public/pkg/cloudwatchmetrics latest
	github.com/spring-media/curation-pkgs-public/pkg/csvexport latest
)

replace (
	github.com/spring-media/curation-pkgs-public/pkg/cloudwatchmetrics => ./pkg/cloudwatchmetrics
	github.com/spring-media/curation-pkgs-public/pkg/cvsexport => ./pkg/csvexport
)
