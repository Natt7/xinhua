create database db;
use db;

create schema jt_w_cgshmx_schema
source type csv
fields (
	 cgshmxid        type string,
	 cgshbdmxid      type string,
	 cgshid          type string,
	 cgdmxid         type string,
	 yxfsid          type string,
	 spxxid          type string,
	 jj              type double,
	 jz              type double,
	 ydjj            type double,
	 ydjz            type double,
	 ydsl            type double,
	 ydsy            type double,
	 ydmy            type double,
	 sssl            type double,
	 sssy            type double,
	 ssmy            type double,
	 zzsl            type double,
	 zzsy            type double,
	 zzmy            type double,
	 yfsl            type double,
	 yfmy            type double,
	 yfsy            type double,
	 bz              type string,
	 zq              type datetime,
	 bh              type string,
	 jxsl            type u64,
	 shqx            type string,
	 khddmxid        type string,
	 wxtxsmxid       type string,
	 wxtxsbdmxid     type string,
	 wxtxsdid        type string,
	 wxtgystzid      type string,
	 wxtgystzmxid    type string,
	 wlshdh          type string,
	 wlshmx          type string,
	 dj              type double,
	 bbid            type string,
	 zhlssj          type string,
	 nocgd           type string,
	 ysdh            type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_w_cgshmx_parser
type rcd
schema jt_w_cgshmx_schema;
create table jt_w_cgshmx using jt_w_cgshmx_parser;
create index jt_w_cgshmx_index on table jt_w_cgshmx(cgshmxid);


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

create schema jt_j_spxx_schema
source type csv
fields (
      spxxid              type string,
      spbh                type string,
      txm                 type string,
      pm                  type string,
      dj                  type double,
      fxflid              type string,
      cbflid              type string,
      cbsid               type string,
      bbid                type string,
      zz                  type string,
      bz                  type string,
      yz                  type string,
      kbid                type string,
      zzid                type string,
      zl                  type double,
      hd                  type double,
      sl                  type double,
      cbny                type string,
      bc                  type string,
      yzid                type string,
      tsid                type string,
      gg                  type string,
      xh                  type string,
      yc                  type double,
      yssj                type string,
      yzs                 type double,
      lrry                type string,
      lrrq                type string,
      tyry                type string,
      tyrq                type string,
      zt                  type string,
      beiz                type string,
      ywlx                type string,
      yxjz                type string,
      yxbz                type string,
      yxdvd               type string,
      yxjxsl              type double,
      yxxxsl              type double,
      yxyz                type string,
      yxpc                type string,
      yxdy                type string,
      yxzyyy              type string,
      csm                 type string,
      ym                  type double,
      spsxid              type string,
      xgrq                type string,
      isbn                type string,
      yxfxs               type string,
      yxjxs               type string,
      xgr                 type string,
      cwdlid              type string,
      xtxm                type string,
      ttmbz               type string,
      gjid                type string,
      spxxkey             type string,
      spsys               type double,
      xtnzy               type string,
      xsbz                type string,
      shuji               type string,
      spsm                type string,
      bwb                 type string,
      zs                  type double,
      gjz                 type string,
      czrq                type string,
      ttmbz2              type string,
      jcjf                type string,
      lydwid              type string,
      ysid                type string,
      cd                  type string,
      bzq                 type double,
      yymarc              type string,
      qh                  type string,
      kh                  type string,
      yfdh                type string,
      mlj                 type double,
      gwkj                type double,
      whpj                type double,
      newsku              type string,
      mbts                type u64,
      mtcs                type u64,
      ztflag              type string,
      xsspflag            type string,
      ptpflag             type string,
      fspflag             type string,
      zspflag             type string,
      zddmxid             type string,
      sbid                type string,
      nrjj                type string,
      fmtp                type string,
      zddm                type string,
	  splbid              type string,
      splxid              type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_spxx_parser
type rcd
schema jt_j_spxx_schema;
create table jt_j_spxx using jt_j_spxx_parser;
create index jt_j_spxx_index on table jt_j_spxx(spxxid);

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

create schema jt_j_cwdl_schema
source type csv
fields (
	 cwdlid     		type string,
	 flbh      			type string,
	 cwfl      			type string,
	 fljc      			type string,
	 zjm      			type string,
	 zt      			type string,
	 cjr      			type string,
	 tyr      			type string,
	 czrq      			type datetime,
	 sl      			type u64
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_cwdl_parser
type rcd
schema jt_j_cwdl_schema;
create table jt_j_cwdl using jt_j_cwdl_parser;
create index jt_j_cwdl_index on table jt_j_cwdl(cwdlid);

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

create schema 1_1_result_schema
source type csv
fields (
	cgshdh     			    type string,
	dgysid     			    type string,
	gysmc     			    type string,
	cwdlid     			    type string,
	cwfl    			    type string,
	rjfxid    			    type string,
	rjflmc       			type string,
	sssl     				type double,
	ssmy     				type double,
	sssy      				type double,
	dhrq      				type datetime
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser 1_1_result_parser
type rcd
schema 1_1_result_schema;
create table 1_1_result using 1_1_result_parser;
create index 1_1_result_index on table 1_1_result(cgshdh); 
create statistics model 1_1_result_sum on table 1_1_result
group by ("cgshdh","dgysid","gysmc","cwdlid","cwfl","rjfxid","rjflmc","dhrq")
measures (sum(sssl),sum(ssmy),sum(sssy));
