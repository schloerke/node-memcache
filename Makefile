
test:
	@ ./node_modules/.bin/nodeunit tests/*.js

watch-tests:
	@ coffee --compile --bare --watch -o test/ test/coffee/*.coffee &

watch-lib:
	@ coffee --compile --bare --watch -o lib/ lib/coffee/*.coffee &

watch: watch-lib watch-tests

