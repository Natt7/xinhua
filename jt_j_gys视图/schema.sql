create database db;
use db;

create schema jt_j_dwxx_schema
source type csv
fields (
	dwid           type string,
	dwbh           type string,
	dwmc           type string,
	dwjc           type string,
	zjm            type string,
	sjdwid         type string,
	dwsxid         type string,
	bmlxid         type string,
	gyslxid        type string,
	khlxid         type string,
	kflxid         type string,
	yinsdwlxid     type string,
	cbslxid        type string,
	ysdwlxid       type string,
	bmztid         type string,
	gzlid          type string,
	dwjb           type u64,
	dwfr           type string,
	sfid           type string,
	dqid           type string,
	yzbm           type string,
	txdz           type string,
	dh             type string,
	cz             type string,
	lxr            type string,
	khyh           type string,
	zh             type string,
	sh             type string,
	email          type string,
	wz             type string,
	jsfsid         type string,
	tssx           type double,
	tsxx           type double,
	sdsx           type double,
	sdxx           type double,
	zksx           type double,
	zkxx           type double,
	jsdwid         type string,
	djsdbz         type string,
	tsjsdbz        type string,
	khzzyfbz       type double,
	khzzbzfbz      type double,
	khzzqtfybz     type double,
	cgjszq         type u64,
	tsjszq         type u64,
	ysfsid         type string,
	zzdbz          type string,
	yzdbh          type string,
	ezdbh          type string,
	tdyzs          type u64,
	dwtjbh         type string,
	cgjsyxjb       type u64,
	xsjsyxjb       type u64,
	phyxjb         type u64,
	xtyxjb         type u64,
	jtyxjb         type u64,
	cgyxjb         type u64,
	zt             type string,
	cjr            type string,
	tyr            type string,
	czrq           type string,
	bz             type string,
	mrxszk         type double,
	mrxsfsid       type string,
	szwzid         type string,
	zdkhfhzk       type double,
	gljy           type string,
	laberflag      type string,
	thttbl         type u64,
	cght           type string,
	sjxz           type string,
	cgfpye         type double,
	gyshzdjid      type string,
	djdyxs         type string,
	jsdwmc         type string,
	kpsx           type string,
	dwxxkey        type string,
	yxzf           type string,
	yxbb           type string,
	fxfsid         type string,
	zzjgdm         type string,
	yyzz           type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_dwxx_parser
type rcd
schema jt_j_dwxx_schema;
create table jt_j_dwxx using jt_j_dwxx_parser;
create index jt_j_dwxx_index on table jt_j_dwxx(dwid);

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