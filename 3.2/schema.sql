create database db;
use db;

create schema Jt_c_Bmspkfmx_schema
source type csv
fields (
	  jtdmxid           type string,
	  jtdpzmxid         type string,
	  jtdid             type string,
	  jttzdmxid         type string,
	  jttzdid           type string,
	  spxxid            type string,
	  jhfsid            type string,
	  dj                type double,
	  jz                type double,
	  jj                type double,
	  thzk              type double,
	  thjg              type double,
	  jtsl              type double,
	  jtsy              type double,
	  jtmy              type double,
	  jtcb              type double,
	  sdsl              type double,
	  sdmy              type double,
	  sdsy              type double,
	  sdcb              type double,
	  zzsl              type double,
	  zzsy              type double,
	  zzmy              type double,
	  yjsl              type double,
	  yjsy              type double,
	  yjmy              type double,
	  bz                type string,
	  bmspkfmxid        type string,
	  gysspmxid         type string,
	  bhqdh             type string,
	  wxttzid           type string,
	  wxttzmxid         type string,
	  wxtfhdmxid        type string,
	  qtdid             type string,
	  qtdmxid           type string,
	  lydjid            type string,
	  lydjmxid          type string,
	  thsl              type u64,
	  yjtdmxid          type string,
	  sn                type string,
	  csbz              type string,
	  bhsjj             type double,
	  bhsjz             type double,
	  bhsjtsy           type double,
	  se                type double,
	  hwid              type string,
	  hwkctzid          type string,
	  sl                type double,
	  xsxh              type double,
	  dzzk              type double,
	  dztj              type double,
	  dzsy              type double,
	  dzcy              type double,
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
	yyzz           type string,
	sfzxck         type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_dwxx_parser
type rcd
schema jt_j_dwxx_schema;
create table jt_j_dwxx using jt_j_dwxx_parser;
create index jt_j_dwxx_index on table jt_j_dwxx(dwid); 
