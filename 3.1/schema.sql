create database db;
use db;

create schema Jt_c_Bmspkfmx_schema
source type csv
fields (
      bmspkfmxid        type string,
	  bmspkftzid        type string,
	  ywbmid            type string,
	  wlbmid            type string,
	  spxxid            type string,
	  gysid             type string,
	  yxfsid            type string,
	  dj                type double,
	  jz                type double,
	  jj                type double,
	  xz                type double,
	  xj                type double,
	  qckc              type double,
	  qcmy              type double,
	  qcsy              type double,
	  jhsl              type double,
	  jhmy              type double,
	  jhsy              type double,
	  xtsl              type double,
	  xtmy              type double,
	  xtsy              type double,
	  drsl              type double,
	  drmy              type double,
	  drsy              type double,
	  xssl              type double,
	  xsmy              type double,
	  xssy              type double,
	  jtsl              type double,
	  jtmy              type double,
	  jtsy              type double,
	  dcsl              type double,
	  dcmy              type double,
	  dcsy              type double,
	  bfsl              type double,
	  bfmy              type double,
	  bfsy              type double,
	  sysl              type double,
	  symy              type double,
	  sysy              type double,
	  qmkc              type double,
	  qmmy              type double,
	  qmsy              type double,
	  jzrq              type datetime,
	  zt                type string,
	  yzt               type string,
	  bz                type string,
	  pfsl              type double,
	  pfmy              type double,
	  pfsy              type double,
	  cgshmxid          type string,
	  ztid              type string,
	  ytsl              type double,
	  ytsy              type double,
	  ytmy              type double,
	  ztbz              type string,
	  zq                type datetime,
	  jxsl              type double,
	  xsztsl            type double,
	  jtztsl            type double,
	  tmsl              type double,
	  xsztsy            type double,
	  xsztmy            type double,
	  jtztsy            type double,
	  jtztmy            type double,
	  ktbz              type string,
	  shdid             type string,
	  tzscrq            type datetime,
	  yksl              type u64,
	  bbid              type string,
	  bcsc              type string,
	  nbztsl            type double,
	  nbztsy            type double,
	  nbztmy            type double,
	  djsl              type double,
	  cssl              type double,
	  cssy              type double,
	  csmy              type double,
	  cszysl            type double,
	  cszysy            type double,
	  cszymy            type double,
	  kykc              type double,
	  kykcmy            type double,
	  kykcsy            type double,
	  tjzysl            type double,
	  tjzymy            type double,
	  tjzysy            type double,
	  pxzysl            type double,
	  pxzymy            type double,
	  pxzysy            type double,
	  zhzysl            type double,
	  zhzymy            type double,
	  zhzysy            type double,
	  bfzysl            type double,
	  bfzymy            type double,
	  bfzysy            type double,
	  bszysl            type double,
	  bszymy            type double,
	  bszysy            type double,
	  flhjj             type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser Jt_c_Bmspkfmx_parser
type rcd
schema Jt_c_Bmspkfmx_schema;
create table Jt_c_Bmspkfmx using Jt_c_Bmspkfmx_parser;
create index Jt_c_Bmspkfmx_index on table Jt_c_Bmspkfmx(xsdmxid);

create schema Jt_c_Khspmx_schema
source type csv
fields (
		fhzzrq           type string,
		vf_dwmc    		 type expr data type string value iilmap("jt_j_dwxx_map",ztid)
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser Jt_c_Khspmx_parser
type rcd
schema Jt_c_Khspmx_schema;
create table Jt_c_Khspmx using Jt_c_Khspmx_parser;
create index Jt_c_Khspmx_index on table Jt_c_Khspmx(xsdmxid);


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
	  vf_cwdlid    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map",fxflid)
	  vf_rjfxid    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map1",fxflid)
	  vf_rjflmc    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map2",fxflid)
	  vf_cwfl    	     type expr data type string value iilmap("jt_j_fxfl_rjfl_map3",fxflid)
                    
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_spxx_parser
type rcd
schema jt_j_spxx_schema;
create table jt_j_spxx using jt_j_spxx_parser;
create index jt_j_spxx_index on table jt_j_spxx(fxflid); 

create schema jt_j_fxfl_rjfl_schema
source type csv
fields (
	fxflid           type string,
	fxflmc           type string,
	rjfxid           type string,
	rjflmc           type string,
	cwdlid           type string,
	vf_cwfl    	     type expr data type string value iilmap("jt_j_cwdl_map",cwdlid)
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
key (fxflid)
value (cwdlid)
type string;
create map jt_j_fxfl_rjfl_map1 on table jt_j_fxfl_rjfl
key (fxflid)
value (rjfxid)
type string;
create map jt_j_fxfl_rjfl_map2 on table jt_j_fxfl_rjfl
key (fxflid)
value (rjflmc)
type string;
create map jt_j_fxfl_rjfl_map3 on table jt_j_fxfl_rjfl
key (fxflid)
value (vf_cwfl)
type string;

create schema jt_j_cwdl_schema
source type csv
fields (
	fxflid           type string,
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser jt_j_cwdl_parser
type rcd
schema jt_j_cwdl_schema;
create table jt_j_cwdl using jt_j_cwdl_parser;
create index jt_j_cwdl_index on table jt_j_cwdl(fxflid);
create map jt_j_cwdl_map on table jt_j_cwdl
key (cwdlid)
value (cwfl)
type string;
