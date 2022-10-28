.PHONY: rsync
rsync:
	rsync -r --filter=':- .gitignore' --exclude=.git . -e "ssh -p 60022" lima@localhost:~/lab/miniob

.PHONY: sync_yacc
sync_yacc:
	scp -P 60022 lima@localhost:~/lab/miniob/src/observer/sql/parser/yacc_sql.tab.* src/observer/sql/parser
	scp -P 60022 lima@localhost:~/lab/miniob/src/observer/sql/parser/lex.yy.* src/observer/sql/parser
