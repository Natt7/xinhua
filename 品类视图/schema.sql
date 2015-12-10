create database db;
use db;

create schema jt_j_fxfl_schema
source type csv
fields (
     fxflid                type string,
	 fxflbh                type string,
	 fxflmc                type string,
	 fxfljc                type string,
	 fxflfid               type string,
	 fxfljs                type u64,
	 zjm                   type string,
	 zt                    type string,
	 cjr                   type string,
	 tyr                   type string,
	 czrq                  type string,
	 cwdlid                type string,
	 tyrq                  type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser jt_j_fxfl_parser
type rcd
schema jt_j_fxfl_schema;
create table jt_j_fxfl using jt_j_fxfl_parser;
create index jt_j_fxfl_index on table jt_j_fxfl(fxflid);

create schema jt_j_fxfl_rjfl_schema
source type csv
fields (
	fxflid           type string,
	fxflmc           type string,
	rjfxid           type string,
	rjflmc           type string,
	cwdlid           type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser jt_j_fxfl_rjfl_parser
type rcd
schema jt_j_fxfl_rjfl_schema;
create table jt_j_fxfl_rjfl using jt_j_fxfl_rjfl_parser;
create index jt_j_fxfl_rjfl_index on table jt_j_fxfl_rjfl(fxflid); 