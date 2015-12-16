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
      ztid                type string
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
		yswjflag         type string,
		rjhzid           type string,
		fhzzflag         type string,
		fhzzrq           type datetime,
		mdxtfpid         type string,
		mdxtfph          type string,
		mdxtfpcdate      type datetime,
		mdxtfpsbrq       type datetime,
		xszcb            type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xsd_sb_parser
type rcd
schema jt_x_xsd_sb_schema;
create table jt_x_xsd_sb using jt_x_xsd_sb_parser;
create index jt_x_xsd_sb_index on table jt_x_xsd_sb(xsdid);


create schema ls_jt_x_xtdmx_schema
source type csv
fields (
	  xtdmxid   	type string,
	  xtdid    		type string,
	  bmspkfmxid    type string,
	  khspmxid     	type string,
	  yxfsid    	type string,
	  zddm    		type string,
	  spxxid    	type string,
	  dj    		type double,
	  xz    		type double,
	  xj    		type double,
	  sl    		type double,
	  xtsl    		type double,
	  xtsy    		type double,
	  xtmy    		type double,
	  yjsl    		type double,
	  yjsy    		type double,
	  yjmy    		type double,
	  qsjsrq    	type datetime,
	  zhjsrq    	type datetime,
	  lydjbid    	type string,
	  lydjid    	type string,
	  lydjmxid     	type string,
	  lybmid    	type string,
	  zt    		type string,
	  yzt    		type string,
	  bz    		type string,
	  zzsl     		type double,
	  zzsy     		type double,
	  zzmy     		type double,
	  xsrq     		type string,
	  ywbmid    	type string,
	  sczt     		type string,
	  gysid    		type string,
	  jz    		type double,
	  jj    		type double,
	  hycode    	type string,
	  zbkhspmxid    type string,
	  ztid     		type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser ls_jt_x_xtdmx_parser
type rcd
schema ls_jt_x_xtdmx_schema;
create table ls_jt_x_xtdmx using ls_jt_x_xtdmx_parser;
create index ls_jt_x_xtdmx_index on table ls_jt_x_xtdmx(xtdmxid);

create schema ls_jt_x_xtd_schema
source type csv
fields (
	 xtdid                     		type string,
	 xtdh                     		type string,
	 xsbmid                      	type string,
	 wlbmid                      	type string,
	 khid                       	type string,
	 ysfsid                      	type string,
	 jsfsid                      	type string,
	 djrq                           type datetime,
	 czyid                        	type string,
	 pzs                            type double,
	 xtsl                           type double,
	 xtsy                           type double,
	 xtmy                           type double,
	 yjsl                           type double,
	 yjsy                           type double,
	 yjmy                           type double,
	 qsjsrq                         type datetime,
	 zhjsrq                         type datetime,
	 czrq                           type datetime,
	 zt                  			type string,
	 yzt                     		type string,
	 bz                    			type string,
	 shrid                     		type string,
	 shdwid                     	type string,
	 ztid                      		type string,
	 zzzsl                          type double,
	 zzzsy                          type double,
	 zzzmy                          type double,
	 sl                             type double,
	 xslx                          	type string,
	 pzh                          	type string,
	 xsrq                          	type datetime,
	 jsztflag  						type string,
	 pzztflag  						type string,
	 xtnth  						type string,
	 hsbmid   						type string,
	 wlshdh   						type string,
	 ydh  							type string,
	 xtfprq   						type datetime,
	 sbrq 							type datetime,
	 clrq 							type datetime,
	 clbz 							type string,
	 yswjflag  						type string,
	 rjhzid                    		type string,
	 fhzzflag    					type string,
	 fhzzrq                         type datetime,
	 mdxtfpid                     	type string,
	 mdxtfph                     	type string,
	 mdxtfpcdate                    type datetime,
	 mdxtfpsbrq                   	type datetime,
	 xtzcb                     		type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser ls_jt_x_xtd_parser
type rcd
schema ls_jt_x_xtd_schema;
create table ls_jt_x_xtd using ls_jt_x_xtd_parser;
create index ls_jt_x_xtd_index on table ls_jt_x_xtd(xtdid);


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

create schema 2_2_result_schema
source type csv
fields (
	 ztid     			type string,
	 dwmc     			type string,
	 cwdlid     		type string,
	 cwfl      			type string,
	 rjfxid      		type string,
	 rjflmc      		type string,
	 xslx      			type string,
	 xsmy      			type double,
	 spxxid      		type string,
	 xsrq      			type datetime
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser 2_2_result_parser
type rcd
schema 2_2_result_schema;
create table 2_2_result using 2_2_result_parser;
create index 2_2_result_index on table 2_2_result(cwdlid);
create statistics model 2_2_result_sum on table 2_2_result
group by ("ztid","dwmc","cwdlid","cwfl","rjfxid","rjflmc","xslx","xsrq")
measures (sum(xsmy),count(DISTINCT spxxid));
