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
	 cwdlid                type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_fxfl_parser
type rcd
schema jt_j_fxfl_schema;
create table jt_j_fxfl using jt_j_fxfl_parser;
create index jt_j_fxfl_index on table jt_j_fxfl(fxflid);

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

create schema jt_x_xsdmx_schema
source type csv
fields (
	 xsdmxid       		type string,
	 xsdbdmxid     		type string,
	 xsdid         		type string,
	 bmspkfmxid    		type string,
	 bjqdh     	  		type string,
	 zddm     	  		type string,
	 spxxid    	  		type string,
	 xsfsid        		type string,
	 dj     		  	type double,
	 xz     		  	type double,
	 xj     		  	type double,
	 jj     		  	type double,
	 jz     		  	type double,
	 gysid     	  		type string,
	 sl     		  	type double,
	 fhsl     	  		type double,
	 fhsy     	  		type double,
	 fhmy     	  		type double,
	 sdsl     	  		type double,
	 sdsy     	  		type double,
	 sdmy     	  		type double,
	 zzsl     	  		type double,
	 zzsy     	  		type double,
	 zzmy     	  		type double,
	 yjsl     	  		type double,
	 yjsy     	  		type double,
	 yjmy     	  		type double,
	 bz     		  	type string,
	 pfdmxid       		type string,
	 pfdid      	  	type string,
	 khddmxid      		type string,
	 khddid        		type string,
	 xtsl     	  		type double,
	 xtsy     	  		type double,
	 xtmy     	  		type double,
	 pxdmxid       		type string,
	 pxdid      	  	type string,
	 posid      	  	type string,
	 cghth      	  	type string,
	 ysdh     	  		type string,
	 wxtcgdid      		type string,
	 wxtcgdzpid    		type string,
	 wxtcgdmxid    		type string,
	 wlmxid     	  	type string,
	 wlzdid     	  	type string,
	 dkkfyid       		type string,
	 kfssbmid      		type string,
	 lxtmxid       		type string,
	 ydsl     	  		type u64,
	 djrq     	  		type datetime,
	 xsrq     	  		type datetime,
	 wlzpdh     	  	type string,
	 wlzpmx     	  	type string,
	 khspmxid      		type string,
	 cylx          		type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xsdmx_parser
type rcd
schema jt_x_xsdmx_schema;
create table jt_x_xsdmx using jt_x_xsdmx_parser;
create index jt_x_xsdmx_index on table jt_x_xsdmx(xsdmxid);

create schema jt_x_xsd_schema
source type csv
fields (
	   xsdid             type string,
	   xsdh              type string,
	   xsywlxid          type string,
	   xslx              type string,
	   xsbmid            type string,
	   wlbmid            type string,
	   khid              type string,
	   shdwid            type string,
	   shdzid            type string,
	   fhydh             type string,
	   wlfhdh            type string,
	   sl                type u64,
	   ysfsid            type string,
	   jsfsid            type string,
	   djrq              type datetime,
	   xsrq              type datetime,
	   czrq              type datetime,
	   czyid             type string,
	   fhpzs             type double,
	   fhzsl             type double,
	   fhzsy             type double,
	   fhzmy             type double,
	   fhbjs             type double,
	   sdpzs             type double,
	   sdzsl             type double,
	   sdzsy             type double,
	   sdzmy             type double,
	   sdbjs             type double,
	   zzzsl             type double,
	   zzzsy             type double,
	   zzzmy             type double,
	   yjzsl             type double,
	   yjzsy             type double,
	   yjzmy             type double,
	   qsjsrq            type datetime,
	   zt                type string,
	   yzt               type string,
	   bz                type string,
	   ztid              type string,
	   pzh               type string,
	   jjfh              type string,
	   sjfhrq            type datetime,
	   lxtzh             type string,
	   pzztflag          type string,
	   jsztflag          type string,
	   xtfprq            type datetime,
	   xtwxsd            type string,
	   b2bqrrq           type datetime,
	   b2bsdzsl          type u64,
	   hsbmid            type string,
	   dbrq              type datetime,
	   senddate          type datetime,
	   hkjzrq            type datetime,
	   tjsqdh            type string,
	   pfdid             type string,
	   pfzjdid           type string,
	   zpfh              type string,
	   wlzpdh            type string,
	   xfrq              type datetime,
	   mdjsrq            type datetime,
	   ykbz              type string,
	   gdbz              type string,
	   dycs              type double,
	   bhsxssy           type double,
	   jjzmy             type double,
	   se                type double,
	   yf                type double,
	   hygsid            type string,
	   dzflag            type string,
	   dzzsy             type double,
	   dzzcy             type double,
	   skzrr             type string,
	   flzje             type double,
	   dzr               type string,
	   dzrq              type datetime,
	   ywlxid            type string,
	   jhzcb             type double,
	   hwjs              type u64,
	   khshr             type string,
	   khdh              type string,
	   ysbh              type string,
	   cwsdbz            type string,
	   cwsdr             type string,
	   cwsdrq            type datetime
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xsd_parser
type rcd
schema jt_x_xsd_schema;
create table jt_x_xsd using jt_x_xsd_parser;
create index jt_x_xsd_index on table jt_x_xsd(xsdid);

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

create schema jt_c_bmspkfmx_schema
source type csv
fields (
	 bmspkfmxid         	type string,
	 bmspkftzid         	type string,
	 ywbmid         		type string,
	 wlbmid         		type string,
	 spxxid         		type string,
	 gysid             		type string,
	 yxfsid             	type string,
	 dj                 	type double,
	 jz                 	type double,
	 jj                 	type double,
	 xz                 	type double,
	 xj                 	type double,
	 qckc                  	type double,
	 qcmy                  	type double,
	 qcsy                  	type double,
	 jhsl                  	type double,
	 jhmy                  	type double,
	 jhsy                  	type double,
	 xtsl                  	type double,
	 xtmy                  	type double,
	 xtsy                  	type double,
	 drsl                  	type double,
	 drmy                  	type double,
	 drsy                  	type double,
	 xssl                  	type double,
	 xsmy                  	type double,
	 xssy                  	type double,
	 jtsl                  	type double,
	 jtmy                  	type double,
	 jtsy                  	type double,
	 dcsl                  	type double,
	 dcmy                  	type double,
	 dcsy                  	type double,
	 bfsl                  	type double,
	 bfmy                  	type double,
	 bfsy                  	type double,
	 sysl                  	type double,
	 symy                  	type double,
	 sysy                  	type double,
	 qmkc                  	type double,
	 qmmy                  	type double,
	 qmsy                  	type double,
	 jzrq                  	type datetime,
	 zt                     type string,
	 yzt                 	type string,
	 bz                 	type string,
	 pfsl                   type double,
	 pfmy                   type double,
	 pfsy                   type double,
	 cgshmxid            	type string,
	 ztid  					type string,
	 ytsl  					type double,
	 ytsy  					type double,
	 ytmy  					type double,
	 ztbz  					type string,
	 zq                     type datetime,
	 jxsl                   type double,
	 xsztsl                 type double,
	 jtztsl                 type double,
	 tmsl                   type double,
	 xsztsy                 type double,
	 xsztmy                 type double,
	 jtztsy                 type double,
	 jtztmy                 type double,
	 ktbz    				type string,
	 shdid                	type string,
	 tzscrq            		type datetime,
	 yksl   				type u64,
	 bbid   				type string,
	 bcsc   				type string,
	 nbztsl                 type double,
	 nbztsy                 type double,
	 nbztmy                 type double,
	 djsl                   type u64,
	 cssl                   type u64,
	 cssy                   type double,
	 csmy                   type double,
	 cszysl                 type double,
	 cszysy                 type double,
	 cszymy                 type double,
	 csdclzysl              type double,
	 csdclzysy              type double,
	 csdclzymy              type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_c_bmspkfmx_parser
type rcd
schema jt_c_bmspkfmx_schema;
create table jt_c_bmspkfmx using jt_c_bmspkfmx_parser;
create index jt_c_bmspkfmx_index on table jt_c_bmspkfmx(bmspkfmxid);

create schema jt_c_khspmx_schema
source type csv
fields (
	 khspmxid                 		type string,
	 khsptzid                 		type string,
	 ywbmid                       	type string,
	 khid                     		type string,
	 bmspkfmxid                   	type string,
	 spxxid                  		type string,
	 dj                             type double,
	 jz                             type double,
	 jj                             type double,
	 qckc                           type double,
	 qcmy                           type double,
	 qcsy                           type double,
	 jhsl                           type double,
	 jhmy                           type double,
	 jhsy                           type double,
	 xssl                           type double,
	 xsmy                           type double,
	 xssy                           type double,
	 jtsl                           type double,
	 jtmy                           type double,
	 jtsy                           type double,
	 qmkc                           type double,
	 qmmy                           type double,
	 qmsy                           type double,
	 jzrq                           type datetime,
	 xsfsid                       	type string,
	 thcs                        	type u64,
	 zt                        		type string,
	 yzt                       		type string,
	 bz                        		type string,
	 yjsl                        	type double,
	 yjsy                        	type double,
	 yjmy                        	type double,
	 ytcs                        	type double,
	 ytmy                        	type double,
	 ytsy                        	type double,
	 xsdmxid                		type string,
	 zzsl                    		type double,
	 zzsy                    		type double,
	 zzmy                    		type double,
	 ztid                    		type string,
	 xsztsl                         type u64,
	 xtztsl                         type u64,
	 cbzk                           type double,
	 cbjg                           type double,
	 xsztsy                     	type double,
	 xsztmy                     	type double,
	 xtztsy                     	type double,
	 xtztmy                     	type double,
	 gysid                      	type string,
	 wlzdid                      	type string,
	 wlmxid                      	type string,
	 spkt         					type string,
	 zqclflag   					type string,
	 xtsl                           type double,
	 xtmy                           type double,
	 xtsy                           type double,
	 bssl                           type double,
	 bsmy                           type double,
	 bssy                           type double,
	 bysl                           type double,
	 bymy                           type double,
	 bysy                           type double,
	 bfsl                           type double,
	 bfmy                           type double,
	 bfsy                           type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_c_khspmx_parser
type rcd
schema jt_c_khspmx_schema;
create table jt_c_khspmx using jt_c_khspmx_parser;
create index jt_c_khspmx_index on table jt_c_khspmx(khspmxid);

create schema jt_g_jtdmx_schema
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
	  csbz              type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_g_jtdmx_parser
type rcd
schema jt_g_jtdmx_schema;
create table jt_g_jtdmx using jt_g_jtdmx_parser;
create index jt_g_jtdmx_index on table jt_g_jtdmx(jtdmxid);

create schema jt_g_jtd_schema
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
	  dzqrrq         type datetime,
	  sfdz         	 type string,
	  dzqrr          type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_g_jtd_parser
type rcd
schema jt_g_jtd_schema;
create table jt_g_jtd using jt_g_jtd_parser;
create index jt_g_jtd_index on table jt_g_jtd(jtdid);

create schema jt_x_xtdmx_schema
source type csv
fields (
	  xtdmxid          type string,
	  xtdpzmxid        type string,
	  xtdid            type string,
	  bmspkfmxid       type string,
	  khspmxid         type string,
	  yxfsid           type string,
	  zddm             type string,
	  spxxid           type string,
	  dj               type double,
	  xz               type double,
	  xj               type double,
	  sl               type double,
	  xtsl             type double,
	  xtsy             type double,
	  xtmy             type double,
	  yjsl             type double,
	  yjsy             type double,
	  yjmy             type double,
	  qsjsrq           type datetime,
	  zhjsrq           type datetime,
	  lydjbid          type string,
	  lydjid           type string,
	  lydjmxid         type string,
	  lybmid           type string,
	  zt               type string,
	  yzt              type string,
	  bz               type string,
	  zzsl             type double,
	  zzsy             type double,
	  zzmy             type double,
	  xsrq             type string,
	  ywbmid           type string,
	  jj               type double,
	  jz               type double,
	  gysid            type string,
	  clsl             type double,
	  qx               type string,
	  ydsl             type double,
	  sn               type string,
	  cszt             type string,
	  hycode           type string,
	  sbbz             type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xtdmx_parser
type rcd
schema jt_x_xtdmx_schema;
create table jt_x_xtdmx using jt_x_xtdmx_parser;
create index jt_x_xtdmx_index on table jt_x_xtdmx(xtdmxid);

create schema jt_x_xtd_schema
source type csv
fields (
	 xtdid                         	type string,
	 xtdh                          	type string,
	 xsbmid                         type string,
	 wlbmid                         type string,
	 khid                          	type string,
	 ysfsid                         type string,
	 jsfsid                         type string,
	 djrq                        	type datetime,
	 czyid                       	type string,
	 pzs                         	type double,
	 xtsl                         	type double,
	 xtsy                         	type double,
	 xtmy                         	type double,
	 yjzsl                        	type double,
	 yjzsy                        	type double,
	 yjzmy                        	type double,
	 qsjsrq                         type datetime,
	 zhjsrq                         type datetime,
	 czrq                          	type datetime,
	 zt                        	 	type string,
	 yzt                         	type string,
	 bz                         	type string,
	 shrid                         	type string,
	 shdwid                         type string,
	 ztid                          	type string,
	 zzzsl                          type double,
	 zzzsy                          type double,
	 zzzmy                          type double,
	 sl                          	type double,
	 xslx                          	type string,
	 pzh                           	type string,
	 xsrq                          	type datetime,
	 jsztflag      					type string,
	 pzztflag      					type string,
	 xtnth         					type string,
	 hsbmid        					type string,
	 wlshdh        					type string,
	 ydh           					type string,
	 xtfprq            				type datetime,
	 sbrq             				type datetime,
	 ycbz             				type string,
	 jzdh             				type string,
	 fhzzflag         				type string,
	 lxtzh            				type string,
	 ysdh             				type string,
	 rjhzid           				type string,
	 xtpzs            				type double,
	 xtzsl            				type double,
	 xtzmy            				type double,
	 xtzsy            				type double,
	 tjfhdh           				type string,
	 tjshdwid         				type string,
	 tjdwid           				type string,
	 tjflag           				type string,
	 senddate         				type datetime,
	 wlshrq           				type datetime,
	 thyyid           				type string,
	 yswjflag         				type string,
	 ykbz             				type string,
	 gdbz             				type string,
	 yktd             				type string,
	 xtntmpxtdid      				type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xtd_parser
type rcd
schema jt_x_xtd_schema;
create table jt_x_xtd using jt_x_xtd_parser;
create index jt_x_xtd_index on table jt_x_xtd(xtdid);

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