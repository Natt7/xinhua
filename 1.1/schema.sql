create database db;
use db;

create schema Jt_w_Cgshmx_schema
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
	 ysdh            type string,
	 vf_Cgshdh    	 type expr data type string value iilmap("Jt_w_Cgsh_map",Cgshid),
	 vf_Dgysid    	 type expr data type string value iilmap("Jt_w_Cgsh_map1",Cgshid)
	 vf_Gysmc    	 type expr data type string value iilmap("Jt_w_Cgsh_map2",Cgshid)
	 vf_Dhrq    	 type expr data type string value iilmap("Jt_w_Cgsh_map3",Cgshid)
	 vf_cwdlid    	 type expr data type string value iilmap("jt_j_spxx_map",Spxxid)
	 vf_rjfxid    	 type expr data type string value iilmap("jt_j_spxx_map1",Spxxid)
	 vf_rjflmc    	 type expr data type string value iilmap("jt_j_spxx_map2",Spxxid)
	 vf_Cwfl    	 type expr data type string value iilmap("jt_j_spxx_map3",Spxxid)
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser Jt_w_Cgshmx_parser
type rcd
schema Jt_w_Cgshmx_schema;
create table Jt_w_Cgshmx using Jt_w_Cgshmx_parser;
create index Jt_w_Cgshmx_index on table Jt_w_Cgshmx(fxflid);
create statistics model Jt_w_Cgshmx_sum on table Jt_w_Cgshmx
group by ("vf_Cgshdh","vf_Dgysid","vf_Gysmc","vf_cwdlid","vf_Cwfl","vf_rjfxid","vf_rjflmc","vf_Dhrq")
measures (sum(Sssl),sum(Ssmy),sum(Sssy));


create schema Jt_w_Cgsh_schema
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
	dzqrr            type string,
	vf_Dgysid    	 type expr data type string value iilmap("Jt_c_Gysys_map",Cgshid),
	vf_Gysmc    	 type expr data type string value iilmap("Jt_c_Gysys_map1",Cgshid)
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser Jt_w_Cgsh_parser
type rcd
schema Jt_w_Cgsh_schema;
create table Jt_w_Cgsh using Jt_w_Cgsh_parser;
create index Jt_w_Cgsh_index on table Jt_w_Cgsh(fxflid);
create map Jt_w_Cgsh_map on table Jt_w_Cgsh
key (Cgshid)
value (Cgshdh)
type string 
create map Jt_w_Cgsh_map1 on table Jt_w_Cgsh
key (Cgshid)
value (vf_Dgysid)
type string 
create map Jt_w_Cgsh_map2 on table Jt_w_Cgsh
key (Cgshid)
value (vf_Gysmc)
type string;
create map Jt_w_Cgsh_map3 on table Jt_w_Cgsh
key (Cgshid)
value (Dhrq)
type string;

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
      spndj               type double,
      spjyxj              type double,
      mdid                type string,
      splbid              type string,
      hhbm                type string,
      sppfjg              type double,
      jldw                type string,
      jyxszk              type double,
      jyzgxszk            type double,
      jyzdxszk            type double,
      zgpfzk              type double,
      zdpfzk              type double,
      sppfzk              type double,
      sfsn                type string,
      bddt                type string,
      bdxt                type string,
      xtftp               type string,
      dtftp               type string,
      xt                  type string,
      dt                  type string,
      nspxxid             type string,
      wjdz                type string,
      bjtj                type string,
      mtpl                type string,
      llcs                type u64,
      spml                type string,
      spys                type u64,
      spjj                type string,
      wsjg                type double,
      wsxz                type double,
	  vf_cwdlid    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map",Fxflid)
	  vf_rjfxid    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map1",Fxflid)
	  vf_rjflmc    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map2",Fxflid)
	  vf_Cwfl    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map3",Fxflid)
                    
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser jt_j_spxx_parser
type rcd
schema jt_j_spxx_schema;
create table jt_j_spxx using jt_j_spxx_parser;
create index jt_j_spxx_index on table jt_j_spxx(fxflid); 
create map jt_j_spxx_map on table jt_j_spxx
key (Spxxid)
value (vf_cwdlid)
type string;
create map jt_j_spxx_map1 on table jt_j_spxx
key (Spxxid)
value (vf_rjfxid)
type string;
create map jt_j_spxx_map2 on table jt_j_spxx
key (Spxxid)
value (vf_rjflmc)
type string;
create map jt_j_spxx_map3 on table jt_j_spxx
key (Spxxid)
value (vf_Cwfl)
type string;

create schema jt_j_fxfl_rjfl_schema
source type csv
fields (
	fxflid           type string,
	fxflmc           type string,
	rjfxid           type string,
	rjflmc           type string,
	cwdlid           type string,
	vf_Cwfl    	     type expr data type string value iilmap("Jt_j_Cwdl_map",Cwdlid)
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser jt_j_fxfl_rjfl_parser
type rcd
schema jt_j_fxfl_rjfl_schema;
create table jt_j_fxfl_rjfl using jt_j_fxfl_rjfl_parser;
create index jt_j_fxfl_rjfl_index on table jt_j_fxfl_rjfl(fxflid); 
create map jt_j_fxfl_rjfl_map on table jt_j_fxfl_rjfl
key (Fxflid)
value (cwdlid)
type string;
create map jt_j_fxfl_rjfl_map1 on table jt_j_fxfl_rjfl
key (Fxflid)
value (rjfxid)
type string;
create map jt_j_fxfl_rjfl_map2 on table jt_j_fxfl_rjfl
key (Fxflid)
value (rjflmc)
type string;
create map jt_j_fxfl_rjfl_map3 on table jt_j_fxfl_rjfl
key (Fxflid)
value (vf_Cwfl)
type string;

create schema Jt_j_Cwdl_schema
source type csv
fields (
	fxflid           type string,
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser Jt_j_Cwdl_parser
type rcd
schema Jt_j_Cwdl_schema;
create table Jt_j_Cwdl using Jt_j_Cwdl_parser;
create index Jt_j_Cwdl_index on table Jt_j_Cwdl(fxflid);
create map Jt_j_Cwdl_map on table Jt_j_Cwdl
key (Cwdlid)
value (Cwfl)
type string;

create schema Jt_c_Gysys_schema
source type csv
fields (
	vf_Gysmc    	 type expr data type string value iilmap("Jt_j_Gys_map",Ygysid)
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser Jt_c_Gysys_parser
type rcd
schema Jt_c_Gysys_schema;
create table Jt_c_Gysys using Jt_c_Gysys_parser;
create index Jt_c_Gysys_index on table Jt_c_Gysys(fxflid); 
create map Jt_c_Gysys_map on table Jt_c_Gysys
key (Ygysid)
value (Dgysid)
type string
create map Jt_c_Gysys_map1 on table Jt_c_Gysys
key (Ygysid)
value (vf_Gysmc)
type string

create schema Jt_j_Gys_schema
source type csv
fields (
	cwdlid           type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser Jt_j_Gys_parser
type rcd
schema Jt_j_Gys_schema;
create table Jt_j_Gys using Jt_j_Gys_parser;
create index Jt_j_Gys_index on table Jt_j_Gys(fxflid); 
create map Jt_j_Gys_map on table Jt_j_Gys
key (Gysid)
value (Gysmc)
type string
