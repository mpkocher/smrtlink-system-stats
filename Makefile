install:
	R -e "library(devtools); install()"

test:
	R -e "library(devtools); install(); test()"

doc:
	R -e "library(devtools); install(); document()"
