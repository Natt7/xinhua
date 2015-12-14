create database db;
use db;

create schema jt_x_xsdmx_sb_schema
source type csv
fields (
      xsdmxid             type string,
      xsdid               type string,
      bmspkfmxid          type string,
      pfdmxid             type string,
      bjqdh               type string,
      zddm                type string,
      spxxid              type string,
      xsfsid              type string,
      dj                  type double,
      xz                  type double,
      xj                  type double,
      sl                  type double,
      xssl                type double,
      xssy                type double,
      xsmy                type double,
      yjsl                type double,
      yjsy                type double,
      yjmy                type double,
      qsjsrq              type string,
      zhjsrq              type string,
      lydjbid             type string,
      lydjid              type string,
      lydjmxid            type string,
      lybmid              type string,
      zt                  type string,
      yzt                 type string,
      bz                  type string,
      khddmxid            type string,
      zzsl                type double,
      zzsy                type double,
      zzmy                type double,
      xsrq                type string,
      ywbmid              type string,
      sczt                type string,
      xtsl                type double,
      xsxh                type double,
      fhsl                type double,
      gysid               type string,
      kfssbmid            type string,
      dkkfyid             type string,
      sdsl                type double,
      sdmy                type double,
      sdsy                type double,
      jz                  type double,
      jj                  type double,
      hycode              type string,
      sbbz                type string,
      khspmxid            type string,
      ztid                type string,
      sn                  type string,
	  vf_dwmc    		  type expr data type string value iilmap("jt_j_dwxx_map",ztid)
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xsdmx_sb_parser
type rcd
schema jt_x_xsdmx_sb_schema;
create table jt_x_xsdmx_sb using jt_x_xsdmx_sb_parser;
create index jt_x_xsdmx_sb_index on table jt_x_xsdmx_sb(xsdmxid);

create schema jt_x_xsd_sb_schema
source type csv
fields (
		xsdid            type string,
		xsdh             type string,
		xsbmid           type string,
		wlbmid           type string,
		khid             type string,
		ysfsid           type string,
		jsfsid           type string,
		djrq             type string,
		czyid            type string,
		pzs              type double,
		xssl             type double,
		xssy             type double,
		xsmy             type double,
		bjs              type double,
		yjsl             type double,
		yjsy             type double,
		yjmy             type double,
		qsjsrq           type string,
		zhjsrq           type string,
		czrq             type string,
		gzdid            type string,
		fhdz             type string,
		clzt             type string,
		zt               type string,
		yzt              type string,
		bz               type string,
		ydh              type string,
		ztid             type string,
		zzzsl            type double,
		zzzsy            type double,
		zzzmy            type double,
		sl               type double,
		xslx             type string,
		pzh              type string,
		xsrq             type string,
		scbj             type string,
		jsztflag         type string,
		pzztflag         type string,
		xtwxsd           type string,
		hkjzrq           type string,
		hsbmid           type string,
		sdzsl            type double,
		sdzmy            type double,
		sdzsy            type double,
		sdpzs            type double,
		wlfhdh           type string,
		fhydh            type string,
		xtfprq           type string,
		sdbjs            type double,
		sbrq             type string,
		clrq             type string,
		eccl             type string,
		ecclrq           type string,
		mdxtfpid         type string,
		mdxtfph          type string,
		mdxtfpcdate      type string,
		mdxtfpsbrq       type string,
		xszcb            type double,
		yswjflag         type string,
		rjhzid           type string,
		fhzzflag         type string,
		fhzzrq           type string,
		vf_dwmc    		 type expr data type string value iilmap("jt_j_dwxx_map",ztid)
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xsd_sb_parser
type rcd
schema jt_x_xsd_sb_schema;
create table jt_x_xsd_sb using jt_x_xsd_sb_parser;
create index jt_x_xsd_sb_index on table jt_x_xsd_sb(xsdmxid);


create schema Ls_Jt_x_Xtdmx_schema
source type csv
fields (
	   cwsdrq            type datetime
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser Ls_Jt_x_Xtdmx_parser
type rcd
schema Ls_Jt_x_Xtdmx_schema;
create table Ls_Jt_x_Xtdmx using Ls_Jt_x_Xtdmx_parser;
create index Ls_Jt_x_Xtdmx_index on table Ls_Jt_x_Xtdmx(xsdid);

create schema Ls_Jt_x_Xtd_schema
source type csv
fields (
	   cwsdrq            type datetime
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";

create parser Ls_Jt_x_Xtd_parser
type rcd
schema Ls_Jt_x_Xtd_schema;
create table Ls_Jt_x_Xtd using Ls_Jt_x_Xtd_parser;
create index Ls_Jt_x_Xtd_index on table Ls_Jt_x_Xtd(xsdid);


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
