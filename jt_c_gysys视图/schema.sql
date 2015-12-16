create database db;
use db;

create schema jt_j_gysys_schema
source type csv
fields (
	 ygysid        type string,
	 dgysid        type string,
	 gysysid   	   type string,
	 zt            type string,
	 czsj          type datetime,
	 czr           type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_gysys_parser
type rcd
schema jt_j_gysys_schema;
create table jt_j_gysys using jt_j_gysys_parser;
create index jt_j_gysys_index on table jt_j_gysys(ygysid);

create schema jt_j_gys_schema
source type csv
fields (
	gysid     			    type string,
	gysbh     			    type string,
	gysmc     			    type string,
	gysjc     			    type string,
	djsdbz    			    type string,
	djsdid    			    type string,
	zjm       				type string,
	wxtbh     				type string,
	ywyid     				type string,
	sfid      				type string,
	dqid      				type string,
	yzbm     				type string,
	dz       				type string,
	dh       				type string,
	cz        				type string,
	lxr       				type string,
	khyh      				type string,
	zh        				type string,
	sh        				type string,
	email     				type string,
	wz        				type string,
	gysfwptid 				type string,
	zdytjfl1  				type string,
	zdytjfl2  				type string,
	zdytjfl3  				type string,
	zt        				type string,
	cjr       				type string,
	tyr       			    type string,
	czrq      			   	type string,
	gyslxid   			    type string,
	dwid      			    type string,
	dwjb      			    type u64,
	yxzf      			    type string,
	sjgysid   			    type string,
	dwsxid    			    type string,
	kpsx      			    type string,
	cgjszq    			    type u64,
	yyzz      			    type string,
	zzjgdm    			    type string,
	jsdwmc    			    type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_gys_parser
type rcd
schema jt_j_gys_schema;
create table jt_j_gys using jt_j_gys_parser;
create index jt_j_gys_index on table jt_j_gys(gysid); 

create schema jt_w_cgsh_schema
source type csv
fields (
	cgshid        	 type string,
	cgshdh           type string,
	ysdh             type string,
	ydh              type string,
	wlshdh           type string,
	bjs              type u64,
	gysid            type string,
	shrid            type string,
	shrq             type datetime,
	cgbmid           type string,
	shwlid           type string,
	shdwid           type string,
	czyid            type string,
	dhrq             type datetime,
	czrq             type datetime,
	zdrq             type datetime,
	ydzpz            type double,
	ydzsl            type double,
	ydzsy            type double,
	ydzmy            type double,
	sszpz            type double,
	sszsl            type double,
	sszsy            type double,
	sszmy            type double,
	zzzsl            type double,
	zzzsy            type double,
	zzzmy            type double,
	ztid             type string,
	yfzsl            type double,
	yfzmy            type double,
	yfzsy            type double,
	zt               type string,
	yzt              type string,
	bz               type string,
	shl              type u64,
	pzh              type string,
	mxhzsy           type double,
	cysy             type double,
	ydrq             type datetime,
	jszq             type u64,
	cgshdlxid        type string,
	wxtxsdid         type string,
	blwc             type string,
	gysqr            type string,
	ecqr             type string,
	bbid             type string,
	lxtzh            type string,
	ecqrrq           type datetime,
	zhpc             type string,
	blwcrq           type datetime,
	sfbhxs           type string,
	pzztflag         type string,
	jsztflag         type string,
	gysywyid         type string,
	ysdhb            type string,
	ysdhq            type string,
	ycshsl           type u64,
	ycshmy           type double,
	dzflag           type string,
	senddate1        type datetime,
	senddate2        type datetime,
	sfdz             type string,
	zdzflag          type string,
	glzt             type string,
	ykbz             type string,
	cwhxrq           type datetime,
	dzqrrq           type datetime,
	dzqrr            type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_w_cgsh_parser
type rcd
schema jt_w_cgsh_schema;
create table jt_w_cgsh using jt_w_cgsh_parser;
create index jt_w_cgsh_index on table jt_w_cgsh(cgshid);

create schema jt_c_gysys_schema
source type csv
fields (
	ygysid     			    type string,
	dgysid     			    type string,
	zt     			    	type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_c_gysys_parser
type rcd
schema jt_c_gysys_schema;
create table jt_c_gysys using jt_c_gysys_parser;
create index jt_c_gysys_index on table jt_c_gysys(ygysid); 