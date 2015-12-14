create database db;
use db;

create schema Jt_g_Jtdmx_schema
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
	  dzcy              type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser Jt_g_Jtdmx_parser
type rcd
schema Jt_g_Jtdmx_schema;
create table Jt_g_Jtdmx using Jt_g_Jtdmx_parser;
create index Jt_g_Jtdmx_index on table Jt_g_Jtdmx(jtdmxid);

create schema Jt_g_Jtd_schema
source type csv
fields (
	  jtdid          type string,
	  jtdh           type string,
	  gysid          type string,
	  gysshdzid      type string,
	  ywbmid         type string,
	  shqrrq         type datetime,
	  wlfhdh         type string,
	  wlydh          type string,
	  ywyid          type string,
	  jtrq           type datetime,
	  ysfsid         type string,
	  wlbmid         type string,
	  czrq           type datetime,
	  czyid          type string,
	  jtpzs          type double,
	  jtzsl          type double,
	  jtzmy          type double,
	  jtzsy          type double,
	  jtzcb          type double,
	  sdpzs          type double,
	  sdzsl          type double,
	  sdzsy          type double,
	  sdzmy          type double,
	  sdzcb          type double,
	  zt             type string,
	  yzt            type string,
	  bz             type string,
	  yjzsl          type double,
	  yjzsy          type double,
	  yjzmy          type double,
	  ztid           type string,
	  shl            type double,
	  zzzsl          type double,
	  zzzsy          type double,
	  zzzmy          type double,
	  pzh            type string,
	  bjs            type u64,
	  thhth          type string,
	  lxtzh          type string,
	  pzztflag       type string,
	  jsztflag       type string,
	  dzflag         type string,
	  sdtbz          type string,
	  senddate       type datetime,
	  gysqr          type string,
	  hgbjs          type u64,
	  djbjs          type u64,
	  ykbz           type string,
	  cwhxrq         type datetime,
	  bhsjtsy        type double,
	  hygsid         type string,
	  yf             type double,
	  se             type double,
	  mdztid         type string,
	  dzzsy          type double,
	  dzzcy          type double,
	  dzr            type string,
	  dzrq           type datetime,
	  xtfprq         type datetime,
	  cwsdbz         type string,
	  cwsdr          type string,
	  cwsdrq         type datetime
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser Jt_c_Khspmx_parser
type rcd
schema Jt_g_Jtd_schema;
create table Jt_g_Jtd using Jt_g_Jtd_parser;
create index Jt_g_Jtd_index on table Jt_g_Jtd(xsdmxid);


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
create index jt_j_gys_index on table jt_j_gys(dwid); 