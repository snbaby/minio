package com.minio.component;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.drinkjava2.jdbpro.SqlItem;
import com.github.drinkjava2.jsqlbox.DB;
import com.github.drinkjava2.jsqlbox.DbContext;
import com.minio.util.Utils;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ApplicationRunnerImpl implements ApplicationRunner {
	@Value("${ESB_DIRECTORY}")
	private String ESB_DIRECTORY;

	@Value("${ESB_YEAR}")
	private String ESB_YEAR;

	public static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public void run(ApplicationArguments args) throws Exception {
		// TODO Auto-generated method stub
		log.info(">>>>>>>>开始执行>>>>>>>>>");
		DbContext ctx = DbContext.getGlobalDbContext();
		log.info(">>开始执行扫描文件夹--{}--",ESB_DIRECTORY);
		List<File> fileList = new ArrayList<File>();
		File esbDirectory = new File(ESB_DIRECTORY);
		Utils.traverFile(esbDirectory, fileList);
		log.info(">>文件夹数量--{}--",fileList.size());
		List<Map<String,Object>> logMapList = ctx.qryMapList("select files_path,files_name from sdi_jxyz_log where folder_name=?",DB.param(ESB_DIRECTORY));
		log.info(">>获取往期--{}--日志",ESB_DIRECTORY);
		log.info(">>往期日志--{}--条",ESB_DIRECTORY);
		for(int i=0;i<fileList.size();i++) {
			File file = fileList.get(i);
			if(!file.getAbsolutePath().endsWith(".json")) {//不是json文件，直接过滤
				continue;
			}
			
			boolean fileLogExist = false;
			for(int j=0;j<logMapList.size();j++) {
				Map<String,Object> logMap = logMapList.get(j);
				if(logMap.get("files_path").toString().equals(file.getAbsolutePath())) {
					fileLogExist = true;
					break;
				}
			}
			if(fileLogExist) {
				continue;
			}
			
			log.debug(">>文件夹--{}--,第--{}--个文件,文件路径--{}--",ESB_DIRECTORY,i,file.getAbsolutePath());
			String jsonStr = "";
			JSONArray jsa = new JSONArray();
			try {
				jsonStr = FileUtils.readFileToString(file);
				jsa = JSONArray.parseArray(jsonStr);
				log.debug(">>文件夹--{}--,第--{}--个文件,文件路径--{}--,开始解析文件",ESB_DIRECTORY,i,file.getAbsolutePath());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error(">>文件夹--{}--,第--{}--个文件,文件路径--{}--,文件解析异常--{}--",ESB_DIRECTORY,i,file.getAbsolutePath(),e.getMessage());
				e.printStackTrace();
			}
			try {
				ctx.nBatchBegin();
				log.debug(">>文件夹--{}--,第--{}--个文件,文件路径--{}--,开始解析,文件内容大小{}",ESB_DIRECTORY,i,file.getAbsolutePath(),jsa.size());
				jsa.forEach(jsbObj -> {
					JSONObject jsb = (JSONObject) jsbObj;
					if (jsb.containsKey("bodys")) {
						JSONArray jsa1 = jsb.getJSONArray("bodys");
						jsa1.forEach(jsbObj1 -> {
							JSONObject jsb1 = (JSONObject) jsbObj1;
							if (jsb1.containsKey("pkp_waybill_more")) {
								JSONArray pkp_waybill_moreJsa = jsb1.getJSONArray("pkp_waybill_more");
								pkp_waybill_moreInsert(pkp_waybill_moreJsa, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_more");
							}
							if (jsb1.containsKey("pkp_waybill_payment")) {
								JSONArray pkp_waybill_paymentJsa = jsb1.getJSONArray("pkp_waybill_payment");
								pkp_waybill_paymentInsert(pkp_waybill_paymentJsa, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_payment");
							}
							if (jsb1.containsKey("pkp_waybill_fee")) {
								JSONArray pkp_waybill_feeJsa = jsb1.getJSONArray("pkp_waybill_fee");
								pkp_waybill_feeInsert(pkp_waybill_feeJsa, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_fee");
							}
							if (jsb1.containsKey("pkp_waybill_biz")) {
								JSONObject pkp_waybill_bizJsb = jsb1.getJSONObject("pkp_waybill_biz");
								pkp_waybill_bizInsert(pkp_waybill_bizJsb, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_biz");
							}
							if (jsb1.containsKey("pkp_waybill_base")) {
								JSONObject pkp_waybill_baseJsb = jsb1.getJSONObject("pkp_waybill_base");
								pkp_waybill_baseInsert(pkp_waybill_baseJsb, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_base");
							}
							if (jsb1.containsKey("pkp_waybill_cargo")) {
								JSONArray pkp_waybill_cargoJsa = jsb1.getJSONArray("pkp_waybill_cargo");
								pkp_waybill_cargoInsert(pkp_waybill_cargoJsa, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_cargo");
							}
							if (jsb1.containsKey("pkp_waybill_package")) {
								JSONArray pkp_waybill_packageJsa = jsb1.getJSONArray("pkp_waybill_package");
								pkp_waybill_packageInsert(pkp_waybill_packageJsa, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_package");
							}
							if (jsb1.containsKey("pkp_waybill_bill")) {
								JSONArray pkp_waybill_billJsa = jsb1.getJSONArray("pkp_waybill_bill");
								pkp_waybill_billInsert(pkp_waybill_billJsa, ctx);
								log.debug(">>解析对象--{}--","pkp_waybill_bill");
							}
						});
					}
				});
			}catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				log.error(">>执行过程错误路径--{}--",file.getAbsolutePath());
				log.error(">>执行过程错误信息--{}--",e.getMessage());
			} finally {
				// TODO: handle finally clause
				ctx.execute("INSERT INTO sdi_jxyz_log(files_path,files_name,folder_name,created_name)VALUES(?,?,?,?)", file.getAbsolutePath(),file.getName(),ESB_DIRECTORY,"王小贱");
				try {
					ctx.nBatchEnd();	
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
					log.error(">>执行提交错误路径--{}--",file.getAbsolutePath());
					log.error(">>执行提交错误信息--{}--",e.getMessage());
					ctx.execute("INSERT INTO sdi_jxyz_log(files_path,files_name,folder_name,created_name,error_msg)VALUES(?,?,?,?,?)", file.getAbsolutePath(),file.getName(),ESB_DIRECTORY,"王小贱",e.getMessage());
				}
				
			}

		};
		
		log.info(">>>>>>>>结束执行>>>>>>>>>");
	}

	public  void pkp_waybill_moreInsert(JSONArray pkp_waybill_moreJsa, DbContext ctx) {
		for (int i = 0; i < pkp_waybill_moreJsa.size(); i++) {
			List<SqlItem> sqlItemList = new ArrayList<SqlItem>();
			JSONObject jsb = pkp_waybill_moreJsa.getJSONObject(i);

			Utils.consInteger(sqlItemList, jsb, "waybill_more_id");
			Utils.consInteger(sqlItemList, jsb, "pkp_waybill_id");
			Utils.consInteger(sqlItemList, jsb, "sub_waybill_id");
			Utils.consStr(sqlItemList, jsb, "waybill_no");
			Utils.consStr(sqlItemList, jsb, "main_waybill_no");
			Utils.consStr(sqlItemList, jsb, "sub_waybill_no");
			Utils.consDecimal(sqlItemList, jsb, "one_bill_total_weight");
			Utils.consInteger(sqlItemList, jsb, "one_bill_total_number");
			Utils.consStr(sqlItemList, jsb, "one_bill_flag");
			Utils.consStr(sqlItemList, jsb, "split_delivery_flag");
			Utils.consDecimal(sqlItemList, jsb, "real_weight");
			Utils.consDecimal(sqlItemList, jsb, "volume_weight");
			Utils.consDecimal(sqlItemList, jsb, "fee_weight");
			Utils.consDecimal(sqlItemList, jsb, "volume");
			Utils.consDecimal(sqlItemList, jsb, "length");
			Utils.consDecimal(sqlItemList, jsb, "width");
			Utils.consDecimal(sqlItemList, jsb, "height");
			Utils.consInteger(sqlItemList, jsb, "serial_no");
			Utils.consDate(sqlItemList, jsb, "biz_occur_date");
			Utils.consStr(sqlItemList, jsb, "post_org_id");
			Utils.consInteger(sqlItemList, jsb, "post_org_no");
			Utils.consStr(sqlItemList, jsb, "org_drds_code");
			Utils.consStr(sqlItemList, jsb, "post_org_name");
			Utils.consStr(sqlItemList, jsb, "is_deleted");
			Utils.consInteger(sqlItemList, jsb, "create_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_created");
			Utils.consInteger(sqlItemList, jsb, "modify_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_modified");
			Utils.consStr(sqlItemList, jsb, "modify_user_name");
			Utils.consStr(sqlItemList, jsb, "create_user_name");

			sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
			sqlItemList.add(DB.notNull("created_by", "王小贱"));
			if (sqlItemList.size() > 1) {
				if (Utils.validate(jsb) && sqlItemList.size() > 0) {
					ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_more_"+ESB_YEAR+"(", sqlItemList.toArray(), ")",
							DB.valuesQuestions());
				}
			}

		}
	}

	public  void pkp_waybill_paymentInsert(JSONArray pkp_waybill_paymentJsa, DbContext ctx) {
		for (int i = 0; i < pkp_waybill_paymentJsa.size(); i++) {
			List<SqlItem> sqlItemList = new ArrayList<SqlItem>();
			JSONObject jsb = pkp_waybill_paymentJsa.getJSONObject(i);

			Utils.consInteger(sqlItemList, jsb, "waybill_payment_id");
			Utils.consInteger(sqlItemList, jsb, "pkp_waybill_id");
			Utils.consStr(sqlItemList, jsb, "waybill_no");
			Utils.consStr(sqlItemList, jsb, "payment_type");
			Utils.consDecimal(sqlItemList, jsb, "payment_amount");
			Utils.consDecimal(sqlItemList, jsb, "union_amount");
			Utils.consStr(sqlItemList, jsb, "tran_id");
			Utils.consStr(sqlItemList, jsb, "payment_account");
			Utils.consDate(sqlItemList, jsb, "payment_date");
			Utils.consStr(sqlItemList, jsb, "payment_state");
			Utils.consStr(sqlItemList, jsb, "payment_link");
			Utils.consStr(sqlItemList, jsb, "for_the_account");
			Utils.consDate(sqlItemList, jsb, "biz_occur_date");
			Utils.consInteger(sqlItemList, jsb, "post_org_id");
			Utils.consStr(sqlItemList, jsb, "post_org_no");
			Utils.consStr(sqlItemList, jsb, "org_drds_code");
			Utils.consStr(sqlItemList, jsb, "post_org_name");
			Utils.consStr(sqlItemList, jsb, "is_deleted");
			Utils.consInteger(sqlItemList, jsb, "create_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_created");
			Utils.consInteger(sqlItemList, jsb, "modify_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_modified");
			Utils.consStr(sqlItemList, jsb, "create_user_name");

			sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
			sqlItemList.add(DB.notNull("created_by", "王小贱"));
			if (sqlItemList.size() > 1) {
				if (Utils.validate(jsb) && sqlItemList.size() > 0) {
					ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_payment_"+ESB_YEAR+"(", sqlItemList.toArray(), ")",
							DB.valuesQuestions());
				}
			}

		}
	}

	public  void pkp_waybill_feeInsert(JSONArray pkp_waybill_feeJsa, DbContext ctx) {
		for (int i = 0; i < pkp_waybill_feeJsa.size(); i++) {
			List<SqlItem> sqlItemList = new ArrayList<SqlItem>();
			JSONObject jsb = pkp_waybill_feeJsa.getJSONObject(i);

			Utils.consStr(sqlItemList, jsb, "waybill_fee_id");
			Utils.consStr(sqlItemList, jsb, "pkp_waybill_id");
			Utils.consStr(sqlItemList, jsb, "waybill_no");
			Utils.consStr(sqlItemList, jsb, "item_no");
			Utils.consStr(sqlItemList, jsb, "item_name");
			Utils.consStr(sqlItemList, jsb, "item_amount");
			Utils.consStr(sqlItemList, jsb, "payment_mode");
			Utils.consStr(sqlItemList, jsb, "biz_occur_date");
			Utils.consStr(sqlItemList, jsb, "post_org_id");
			Utils.consStr(sqlItemList, jsb, "post_org_no");
			Utils.consStr(sqlItemList, jsb, "org_drds_code");
			Utils.consStr(sqlItemList, jsb, "post_org_name");
			Utils.consStr(sqlItemList, jsb, "is_deleted");
			Utils.consStr(sqlItemList, jsb, "create_user_id");
			Utils.consStr(sqlItemList, jsb, "gmt_created");
			Utils.consStr(sqlItemList, jsb, "modify_user_id");
			Utils.consStr(sqlItemList, jsb, "gmt_modified");
			Utils.consStr(sqlItemList, jsb, "modify_user_name");
			Utils.consStr(sqlItemList, jsb, "create_user_name");

			sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
			sqlItemList.add(DB.notNull("created_by", "王小贱"));
			if (sqlItemList.size() > 1) {
				if (Utils.validate(jsb) && sqlItemList.size() > 0) {
					ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_fee_"+ESB_YEAR+"(", sqlItemList.toArray(), ")",
							DB.valuesQuestions());
				}
			}

		}
	}

	public  void pkp_waybill_bizInsert(JSONObject jsb, DbContext ctx) {
		List<SqlItem> sqlItemList = new ArrayList<SqlItem>();

		Utils.consInteger(sqlItemList, jsb, "waybill_biz_id");
		Utils.consInteger(sqlItemList, jsb, "pkp_waybill_id");
		Utils.consInteger(sqlItemList, jsb, "order_id");
		Utils.consInteger(sqlItemList, jsb, "base_product_id");
		Utils.consStr(sqlItemList, jsb, "base_product_no");
		Utils.consStr(sqlItemList, jsb, "base_product_name");
		Utils.consInteger(sqlItemList, jsb, "biz_product_id");
		Utils.consStr(sqlItemList, jsb, "biz_product_no");
		Utils.consStr(sqlItemList, jsb, "biz_product_name");
		Utils.consStr(sqlItemList, jsb, "io_type");
		Utils.consStr(sqlItemList, jsb, "waybill_no");
		Utils.consDate(sqlItemList, jsb, "biz_occur_date");
		Utils.consInteger(sqlItemList, jsb, "post_org_id");
		Utils.consStr(sqlItemList, jsb, "post_org_no");
		Utils.consStr(sqlItemList, jsb, "org_drds_code");
		Utils.consStr(sqlItemList, jsb, "post_org_name");
		Utils.consInteger(sqlItemList, jsb, "post_person_id");
		Utils.consStr(sqlItemList, jsb, "post_person_no");
		Utils.consStr(sqlItemList, jsb, "post_person_name");
		Utils.consStr(sqlItemList, jsb, "post_person_mobile");
		Utils.consInteger(sqlItemList, jsb, "pickup_person_id");
		Utils.consStr(sqlItemList, jsb, "pickup_person_no");
		Utils.consStr(sqlItemList, jsb, "pickup_person_name");
		Utils.consStr(sqlItemList, jsb, "pickup_person_mobile");
		Utils.consInteger(sqlItemList, jsb, "teamwk_pickup_person_id");
		Utils.consStr(sqlItemList, jsb, "teamwk_pickup_person_no");
		Utils.consStr(sqlItemList, jsb, "teamwk_pickup_person_name");
		Utils.consStr(sqlItemList, jsb, "teamwk_pickup_person_mobile");
		Utils.consDate(sqlItemList, jsb, "pickup_date");
		Utils.consDate(sqlItemList, jsb, "shuttle_bus_date");
		Utils.consStr(sqlItemList, jsb, "sender_type");
		Utils.consInteger(sqlItemList, jsb, "sender_id");
		Utils.consStr(sqlItemList, jsb, "sender_no");
		Utils.consStr(sqlItemList, jsb, "sender");
		Utils.consInteger(sqlItemList, jsb, "sender_warehouse_id");
		Utils.consStr(sqlItemList, jsb, "sender_warehouse_name");
		Utils.consStr(sqlItemList, jsb, "sender_linker");
		Utils.consStr(sqlItemList, jsb, "sender_fixtel");
		Utils.consStr(sqlItemList, jsb, "sender_mobile");
		Utils.consStr(sqlItemList, jsb, "sender_im_type");
		Utils.consStr(sqlItemList, jsb, "sender_im_id");
		Utils.consStr(sqlItemList, jsb, "sender_addr");
		Utils.consStr(sqlItemList, jsb, "sender_addr_additional");
		Utils.consStr(sqlItemList, jsb, "sender_district_no");
		Utils.consStr(sqlItemList, jsb, "sender_postcode");
		Utils.consStr(sqlItemList, jsb, "sender_notes");
		Utils.consDecimal(sqlItemList, jsb, "real_weight");
		Utils.consStr(sqlItemList, jsb, "post_state");
		Utils.consStr(sqlItemList, jsb, "post_notes");
		Utils.consStr(sqlItemList, jsb, "allow_sealing_flag");
		Utils.consDate(sqlItemList, jsb, "expect_sealing_date");
		Utils.consStr(sqlItemList, jsb, "allow_deliver_flag");
		Utils.consStr(sqlItemList, jsb, "need_reapply_type_flag");
		Utils.consStr(sqlItemList, jsb, "reapply_type_fulfil_flag");
		Utils.consStr(sqlItemList, jsb, "post_batch_id");
		Utils.consStr(sqlItemList, jsb, "one_bill_flag");
		Utils.consStr(sqlItemList, jsb, "one_bill_main_waybill_no");
		Utils.consStr(sqlItemList, jsb, "one_bill_fee_type");
		Utils.consStr(sqlItemList, jsb, "distributive_center_no");
		Utils.consStr(sqlItemList, jsb, "distributive_center_name");
		Utils.consStr(sqlItemList, jsb, "provincial_export_center_org_no");
		Utils.consStr(sqlItemList, jsb, "provincial_export_center_org_name");
		Utils.consStr(sqlItemList, jsb, "export_center_org_no");
		Utils.consStr(sqlItemList, jsb, "export_center_org_name");
		Utils.consStr(sqlItemList, jsb, "provincial_import_central_org_no");
		Utils.consStr(sqlItemList, jsb, "provincial_import_central_org_name");
		Utils.consStr(sqlItemList, jsb, "import_central_org_no");
		Utils.consStr(sqlItemList, jsb, "import_central_org_name");
		Utils.consStr(sqlItemList, jsb, "receiver_country_no");
		Utils.consStr(sqlItemList, jsb, "receiver_country_name");
		Utils.consStr(sqlItemList, jsb, "receiver_arrive_org_no");
		Utils.consStr(sqlItemList, jsb, "receiver_arrive_org_name");
		Utils.consStr(sqlItemList, jsb, "receiver_seal_arrive_org_no");
		Utils.consStr(sqlItemList, jsb, "receiver_seal_arrive_org_name");
		Utils.consStr(sqlItemList, jsb, "seal_arrive_org_no");
		Utils.consStr(sqlItemList, jsb, "seal_arrive_org_name");
		Utils.consStr(sqlItemList, jsb, "seal_arrive_area_no");
		Utils.consStr(sqlItemList, jsb, "deliver_org_no");
		Utils.consStr(sqlItemList, jsb, "deliver_org_name");
		Utils.consStr(sqlItemList, jsb, "deliver_section");
		Utils.consStr(sqlItemList, jsb, "deliver_section_name");
		Utils.consStr(sqlItemList, jsb, "deliver_take_org_no");
		Utils.consStr(sqlItemList, jsb, "deliver_take_org_name");
		Utils.consStr(sqlItemList, jsb, "export_customs_no");
		Utils.consStr(sqlItemList, jsb, "export_customs_name");
		Utils.consStr(sqlItemList, jsb, "export_interchange_no");
		Utils.consStr(sqlItemList, jsb, "export_interchange_name");
		Utils.consStr(sqlItemList, jsb, "the_org_grid_no");
		Utils.consStr(sqlItemList, jsb, "the_org_grid_name");
		Utils.consStr(sqlItemList, jsb, "export_grid_no");
		Utils.consStr(sqlItemList, jsb, "export_grid_name");
		Utils.consStr(sqlItemList, jsb, "import_grid_no");
		Utils.consStr(sqlItemList, jsb, "import_grid_name");
		Utils.consStr(sqlItemList, jsb, "straight_seal_flag");
		Utils.consStr(sqlItemList, jsb, "this_turn_flag");
		Utils.consStr(sqlItemList, jsb, "single_seal_flag");
		Utils.consStr(sqlItemList, jsb, "sorting_code");
		Utils.consStr(sqlItemList, jsb, "wms_out_batch_no");
		Utils.consStr(sqlItemList, jsb, "special_handling_transportation");
		Utils.consStr(sqlItemList, jsb, "processing_attribute1");
		Utils.consStr(sqlItemList, jsb, "processing_attribute2");
		Utils.consStr(sqlItemList, jsb, "pickup_attribute");
		Utils.consStr(sqlItemList, jsb, "deliver_attribute");
		Utils.consStr(sqlItemList, jsb, "workbench");
		Utils.consStr(sqlItemList, jsb, "product_reach_area");
		Utils.consStr(sqlItemList, jsb, "contents_attribute");
		Utils.consStr(sqlItemList, jsb, "is_deleted");
		Utils.consInteger(sqlItemList, jsb, "create_user_id");
		Utils.consDate(sqlItemList, jsb, "gmt_created");
		Utils.consInteger(sqlItemList, jsb, "modify_user_id");
		Utils.consDate(sqlItemList, jsb, "gmt_modified");
		Utils.consStr(sqlItemList, jsb, "mailbag_class_code");
		Utils.consStr(sqlItemList, jsb, "direct_name");
		Utils.consStr(sqlItemList, jsb, "post_org_product_name");
		Utils.consStr(sqlItemList, jsb, "declare_curr_code");
		Utils.consStr(sqlItemList, jsb, "gather_flag");
		Utils.consStr(sqlItemList, jsb, "post_org_simple_name");
		Utils.consStr(sqlItemList, jsb, "CREATE_USER_NAME");
		Utils.consStr(sqlItemList, jsb, "direct_no");
		Utils.consStr(sqlItemList, jsb, "business_product_name");
		Utils.consStr(sqlItemList, jsb, "declare_type");
		Utils.consStr(sqlItemList, jsb, "mailbag_class_name");
		Utils.consStr(sqlItemList, jsb, "container_name");
		Utils.consStr(sqlItemList, jsb, "container_no");
		Utils.consStr(sqlItemList, jsb, "is_grid_checked");
		Utils.consStr(sqlItemList, jsb, "gather_area_code");
		Utils.consStr(sqlItemList, jsb, "modify_user_name");
		Utils.consStr(sqlItemList, jsb, "business_product_code");

		sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
		sqlItemList.add(DB.notNull("created_by", "王小贱"));
		if (sqlItemList.size() > 1) {
			if (Utils.validate(jsb) && sqlItemList.size() > 0) {
				ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_biz_"+ESB_YEAR+"(", sqlItemList.toArray(), ")", DB.valuesQuestions());
			}
		}

	}

	public  void pkp_waybill_baseInsert(JSONObject jsb, DbContext ctx) {
		List<SqlItem> sqlItemList = new ArrayList<SqlItem>();

		Utils.consInteger(sqlItemList, jsb, "pkp_waybill_id");
		Utils.consInteger(sqlItemList, jsb, "order_id");
		Utils.consStr(sqlItemList, jsb, "logistics_order_no");
		Utils.consStr(sqlItemList, jsb, "inner_channel");
		Utils.consInteger(sqlItemList, jsb, "base_product_id");
		Utils.consStr(sqlItemList, jsb, "base_product_no");
		Utils.consStr(sqlItemList, jsb, "base_product_name");
		Utils.consInteger(sqlItemList, jsb, "biz_product_id");
		Utils.consStr(sqlItemList, jsb, "biz_product_no");
		Utils.consStr(sqlItemList, jsb, "biz_product_name");
		Utils.consStr(sqlItemList, jsb, "product_type");
		Utils.consStr(sqlItemList, jsb, "product_reach_area");
		Utils.consStr(sqlItemList, jsb, "contents_attribute");
		Utils.consStr(sqlItemList, jsb, "cmd_code");
		Utils.consStr(sqlItemList, jsb, "manual_charge_reason");
		Utils.consStr(sqlItemList, jsb, "time_limit");
		Utils.consStr(sqlItemList, jsb, "io_type");
		Utils.consStr(sqlItemList, jsb, "ecommerce_no");
		Utils.consStr(sqlItemList, jsb, "waybill_type");
		Utils.consStr(sqlItemList, jsb, "waybill_no");
		Utils.consStr(sqlItemList, jsb, "pre_waybill_no");
		Utils.consDate(sqlItemList, jsb, "biz_occur_date");
		Utils.consInteger(sqlItemList, jsb, "post_org_id");
		Utils.consStr(sqlItemList, jsb, "post_org_no");
		Utils.consStr(sqlItemList, jsb, "org_drds_code");
		Utils.consStr(sqlItemList, jsb, "post_org_name");
		Utils.consInteger(sqlItemList, jsb, "post_person_id");
		Utils.consStr(sqlItemList, jsb, "post_person_no");
		Utils.consStr(sqlItemList, jsb, "post_person_name");
		Utils.consStr(sqlItemList, jsb, "post_person_mobile");
		Utils.consStr(sqlItemList, jsb, "sender_type");
		Utils.consInteger(sqlItemList, jsb, "sender_id");
		Utils.consStr(sqlItemList, jsb, "sender_no");
		Utils.consStr(sqlItemList, jsb, "sender");
		Utils.consInteger(sqlItemList, jsb, "sender_warehouse_id");
		Utils.consStr(sqlItemList, jsb, "sender_warehouse_name");
		Utils.consStr(sqlItemList, jsb, "sender_linker");
		Utils.consStr(sqlItemList, jsb, "sender_fixtel");
		Utils.consStr(sqlItemList, jsb, "sender_mobile");
		Utils.consStr(sqlItemList, jsb, "sender_im_type");
		Utils.consStr(sqlItemList, jsb, "sender_im_id");
		Utils.consStr(sqlItemList, jsb, "sender_id_type");
		Utils.consStr(sqlItemList, jsb, "sender_id_no");
		Utils.consStr(sqlItemList, jsb, "sender_id_encrypted_code");
		Utils.consStr(sqlItemList, jsb, "sender_agent_id_type");
		Utils.consStr(sqlItemList, jsb, "sender_agent_id_no");
		Utils.consStr(sqlItemList, jsb, "sender_id_encrypted_code_agent");
		Utils.consStr(sqlItemList, jsb, "sender_addr");
		Utils.consStr(sqlItemList, jsb, "sender_addr_additional");
		Utils.consStr(sqlItemList, jsb, "sender_country_no");
		Utils.consStr(sqlItemList, jsb, "sender_country_name");
		Utils.consStr(sqlItemList, jsb, "sender_province_no");
		Utils.consStr(sqlItemList, jsb, "sender_province_name");
		Utils.consStr(sqlItemList, jsb, "sender_city_no");
		Utils.consStr(sqlItemList, jsb, "sender_city_name");
		Utils.consStr(sqlItemList, jsb, "sender_county_no");
		Utils.consStr(sqlItemList, jsb, "sender_county_name");
		Utils.consStr(sqlItemList, jsb, "sender_district_no");
		Utils.consStr(sqlItemList, jsb, "sender_postcode");
		Utils.consStr(sqlItemList, jsb, "sender_gis");
		Utils.consStr(sqlItemList, jsb, "sender_notes");
		Utils.consStr(sqlItemList, jsb, "registered_customer_no");
		Utils.consStr(sqlItemList, jsb, "receiver_type");
		Utils.consInteger(sqlItemList, jsb, "receiver_id");
		Utils.consStr(sqlItemList, jsb, "receiver_no");
		Utils.consStr(sqlItemList, jsb, "receiver");
		Utils.consInteger(sqlItemList, jsb, "receiver_warehouse_id");
		Utils.consStr(sqlItemList, jsb, "receiver_warehouse_name");
		Utils.consStr(sqlItemList, jsb, "receiver_linker");
		Utils.consStr(sqlItemList, jsb, "receiver_im_type");
		Utils.consStr(sqlItemList, jsb, "receiver_im_id");
		Utils.consStr(sqlItemList, jsb, "receiver_fixtel");
		Utils.consStr(sqlItemList, jsb, "receiver_mobile");
		Utils.consStr(sqlItemList, jsb, "receiver_addr");
		Utils.consStr(sqlItemList, jsb, "receiver_addr_additional");
		Utils.consStr(sqlItemList, jsb, "receiver_country_no");
		Utils.consStr(sqlItemList, jsb, "receiver_country_name");
		Utils.consStr(sqlItemList, jsb, "receiver_province_no");
		Utils.consStr(sqlItemList, jsb, "receiver_province_name");
		Utils.consStr(sqlItemList, jsb, "receiver_city_no");
		Utils.consStr(sqlItemList, jsb, "receiver_city_name");
		Utils.consStr(sqlItemList, jsb, "receiver_county_no");
		Utils.consStr(sqlItemList, jsb, "receiver_county_name");
		Utils.consStr(sqlItemList, jsb, "receiver_district_no");
		Utils.consStr(sqlItemList, jsb, "receiver_postcode");
		Utils.consStr(sqlItemList, jsb, "receiver_gis");
		Utils.consStr(sqlItemList, jsb, "receiver_notes");
		Utils.consInteger(sqlItemList, jsb, "customer_manager_id");
		Utils.consStr(sqlItemList, jsb, "customer_manager_no");
		Utils.consStr(sqlItemList, jsb, "customer_manager_name");
		Utils.consInteger(sqlItemList, jsb, "salesman_id");
		Utils.consStr(sqlItemList, jsb, "salesman_no");
		Utils.consStr(sqlItemList, jsb, "salesman_name");
		Utils.consDecimal(sqlItemList, jsb, "order_weight");
		Utils.consDecimal(sqlItemList, jsb, "real_weight");
		Utils.consDecimal(sqlItemList, jsb, "fee_weight");
		Utils.consDecimal(sqlItemList, jsb, "volume_weight");
		Utils.consDecimal(sqlItemList, jsb, "volume");
		Utils.consDecimal(sqlItemList, jsb, "length");
		Utils.consDecimal(sqlItemList, jsb, "width");
		Utils.consDecimal(sqlItemList, jsb, "height");
		Utils.consInteger(sqlItemList, jsb, "quantity");
		Utils.consStr(sqlItemList, jsb, "packaging");
		Utils.consStr(sqlItemList, jsb, "package_material");
		Utils.consStr(sqlItemList, jsb, "goods_desc");
		Utils.consStr(sqlItemList, jsb, "contents_type_no");
		Utils.consStr(sqlItemList, jsb, "contents_type_name");
		Utils.consDecimal(sqlItemList, jsb, "contents_weight");
		Utils.consInteger(sqlItemList, jsb, "contents_quantity");
		Utils.consStr(sqlItemList, jsb, "cod_flag");
		Utils.consDecimal(sqlItemList, jsb, "cod_amount");
		Utils.consStr(sqlItemList, jsb, "receipt_flag");
		Utils.consStr(sqlItemList, jsb, "receipt_waybill_no");
		Utils.consDecimal(sqlItemList, jsb, "receipt_fee_amount");
		Utils.consStr(sqlItemList, jsb, "insurance_flag");
		Utils.consDecimal(sqlItemList, jsb, "insurance_amount");
		Utils.consDecimal(sqlItemList, jsb, "insurance_premium_amount");
		Utils.consStr(sqlItemList, jsb, "valuable_flag");
		Utils.consStr(sqlItemList, jsb, "transfer_type");
		Utils.consStr(sqlItemList, jsb, "pickup_type");
		Utils.consStr(sqlItemList, jsb, "allow_fee_flag");
		Utils.consStr(sqlItemList, jsb, "is_feed_flag");
		Utils.consDate(sqlItemList, jsb, "fee_date");
		Utils.consDecimal(sqlItemList, jsb, "postage_total");
		Utils.consDecimal(sqlItemList, jsb, "postage_standard");
		Utils.consDecimal(sqlItemList, jsb, "postage_paid");
		Utils.consDecimal(sqlItemList, jsb, "postage_other");
		Utils.consStr(sqlItemList, jsb, "payment_mode");
		Utils.consDecimal(sqlItemList, jsb, "discount_rate");
		Utils.consStr(sqlItemList, jsb, "settlement_mode");
		Utils.consStr(sqlItemList, jsb, "payment_state");
		Utils.consDate(sqlItemList, jsb, "payment_date");
		Utils.consStr(sqlItemList, jsb, "payment_id");
		Utils.consStr(sqlItemList, jsb, "is_advance_flag");
		Utils.consStr(sqlItemList, jsb, "deliver_type");
		Utils.consStr(sqlItemList, jsb, "deliver_sign");
		Utils.consStr(sqlItemList, jsb, "deliver_date");
		Utils.consStr(sqlItemList, jsb, "deliver_notes");
		Utils.consDate(sqlItemList, jsb, "deliver_pre_date");
		Utils.consStr(sqlItemList, jsb, "battery_flag");
		Utils.consStr(sqlItemList, jsb, "workbench");
		Utils.consStr(sqlItemList, jsb, "electronic_preferential_no");
		Utils.consDecimal(sqlItemList, jsb, "electronic_preferential_amount");
		Utils.consStr(sqlItemList, jsb, "pickup_attribute");
		Utils.consStr(sqlItemList, jsb, "adjust_type");
		Utils.consDecimal(sqlItemList, jsb, "postage_revoke");
		Utils.consStr(sqlItemList, jsb, "print_flag");
		Utils.consDate(sqlItemList, jsb, "print_date");
		Utils.consInteger(sqlItemList, jsb, "print_times");
		Utils.consStr(sqlItemList, jsb, "is_deleted");
		Utils.consInteger(sqlItemList, jsb, "create_user_id");
		Utils.consDate(sqlItemList, jsb, "gmt_created");
		Utils.consInteger(sqlItemList, jsb, "modify_user_id");
		Utils.consDate(sqlItemList, jsb, "gmt_modified");
		Utils.consStr(sqlItemList, jsb, "declare_source");
		Utils.consStr(sqlItemList, jsb, "declare_type");
		Utils.consStr(sqlItemList, jsb, "declare_curr_code");
		Utils.consStr(sqlItemList, jsb, "post_org_product_name");
		Utils.consStr(sqlItemList, jsb, "fee_area_suite_code");
		Utils.consStr(sqlItemList, jsb, "post_batch_id");
		Utils.consStr(sqlItemList, jsb, "post_org_simple_name");
		Utils.consStr(sqlItemList, jsb, "fee_area_name");
		Utils.consStr(sqlItemList, jsb, "cargo_total_purchasing_price");
		Utils.consStr(sqlItemList, jsb, "fee_area_code");
		Utils.consStr(sqlItemList, jsb, "postage_suite_code");
		Utils.consStr(sqlItemList, jsb, "create_user_name");
		Utils.consStr(sqlItemList, jsb, "cargo_total_price");
		Utils.consStr(sqlItemList, jsb, "biz_product_type");
		Utils.consStr(sqlItemList, jsb, "modify_user_name");
		Utils.consStr(sqlItemList, jsb, "manage_org_code");
		Utils.consStr(sqlItemList, jsb, "contents_cargo_no");
		Utils.consStr(sqlItemList, jsb, "is_special_marketing");
		Utils.consStr(sqlItemList, jsb, "manual_fee_type");

		sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
		sqlItemList.add(DB.notNull("created_by", "王小贱"));
		if (sqlItemList.size() > 1) {
			if (Utils.validate(jsb) && sqlItemList.size() > 0) {
				ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_base_"+ESB_YEAR+"(", sqlItemList.toArray(), ")",
						DB.valuesQuestions());
			}
		}
	}

	public  void pkp_waybill_cargoInsert(JSONArray pkp_waybill_cargoJsa, DbContext ctx) {
		for (int i = 0; i < pkp_waybill_cargoJsa.size(); i++) {
			List<SqlItem> sqlItemList = new ArrayList<SqlItem>();
			JSONObject jsb = pkp_waybill_cargoJsa.getJSONObject(i);

			Utils.consInteger(sqlItemList, jsb, "waybill_cargo_id");
			Utils.consInteger(sqlItemList, jsb, "pkp_waybill_id");
			Utils.consStr(sqlItemList, jsb, "waybill_no");
			Utils.consStr(sqlItemList, jsb, "cargo_serial");
			Utils.consStr(sqlItemList, jsb, "cargo_type_no");
			Utils.consStr(sqlItemList, jsb, "cargo_type_name");
			Utils.consStr(sqlItemList, jsb, "cargo_category_id");
			Utils.consStr(sqlItemList, jsb, "cargo_category_name");
			Utils.consStr(sqlItemList, jsb, "cargo_no");
			Utils.consStr(sqlItemList, jsb, "cargo_name");
			Utils.consStr(sqlItemList, jsb, "cargo_name_en");
			Utils.consStr(sqlItemList, jsb, "cargo_origin_no");
			Utils.consStr(sqlItemList, jsb, "cargo_origin_name");
			Utils.consStr(sqlItemList, jsb, "cargo_barcode");
			Utils.consStr(sqlItemList, jsb, "cargo_measure_unit");
			Utils.consStr(sqlItemList, jsb, "status");
			Utils.consDecimal(sqlItemList, jsb, "volume");
			Utils.consDecimal(sqlItemList, jsb, "weight");
			Utils.consInteger(sqlItemList, jsb, "quantity");
			Utils.consStr(sqlItemList, jsb, "packaging");
			Utils.consStr(sqlItemList, jsb, "package_status");
			Utils.consStr(sqlItemList, jsb, "move_notes");
			Utils.consDecimal(sqlItemList, jsb, "cargo_value");
			Utils.consDecimal(sqlItemList, jsb, "cargo_price");
			Utils.consDecimal(sqlItemList, jsb, "cargo_purchasing_price");
			Utils.consDecimal(sqlItemList, jsb, "cargo_total_price");
			Utils.consDecimal(sqlItemList, jsb, "cargo_total_purchasing_price");
			Utils.consStr(sqlItemList, jsb, "notes");
			Utils.consDate(sqlItemList, jsb, "biz_occur_date");
			Utils.consInteger(sqlItemList, jsb, "post_org_id");
			Utils.consStr(sqlItemList, jsb, "post_org_no");
			Utils.consStr(sqlItemList, jsb, "org_drds_code");
			Utils.consStr(sqlItemList, jsb, "post_org_name");
			Utils.consStr(sqlItemList, jsb, "is_deleted");
			Utils.consInteger(sqlItemList, jsb, "create_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_created");
			Utils.consInteger(sqlItemList, jsb, "modify_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_modified");
			Utils.consStr(sqlItemList, jsb, "modify_user_name");
			Utils.consStr(sqlItemList, jsb, "cargo_type_name_en");
			Utils.consStr(sqlItemList, jsb, "create_user_name");

			sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
			sqlItemList.add(DB.notNull("created_by", "王小贱"));
			if (sqlItemList.size() > 1) {
				if (Utils.validate(jsb) && sqlItemList.size() > 0) {
					ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_cargo_"+ESB_YEAR+"(", sqlItemList.toArray(), ")",
							DB.valuesQuestions());
				}
			}

		}
	}

	public  void pkp_waybill_packageInsert(JSONArray pkp_waybill_packageJsa, DbContext ctx) {
		for (int i = 0; i < pkp_waybill_packageJsa.size(); i++) {
			List<SqlItem> sqlItemList = new ArrayList<SqlItem>();
			JSONObject jsb = pkp_waybill_packageJsa.getJSONObject(i);

			Utils.consInteger(sqlItemList, jsb, "waybill_package_id");
			Utils.consInteger(sqlItemList, jsb, "pkp_waybill_id");
			Utils.consStr(sqlItemList, jsb, "waybill_no");
			Utils.consStr(sqlItemList, jsb, "package_material_type_no");
			Utils.consStr(sqlItemList, jsb, "package_material_type_name");
			Utils.consStr(sqlItemList, jsb, "package_material_no");
			Utils.consStr(sqlItemList, jsb, "package_material_name");
			Utils.consInteger(sqlItemList, jsb, "quantity");
			Utils.consDecimal(sqlItemList, jsb, "package_material_price");
			Utils.consStr(sqlItemList, jsb, "package_material_spec");
			Utils.consDate(sqlItemList, jsb, "biz_occur_date");
			Utils.consInteger(sqlItemList, jsb, "post_org_id");
			Utils.consStr(sqlItemList, jsb, "post_org_no");
			Utils.consStr(sqlItemList, jsb, "org_drds_code");
			Utils.consStr(sqlItemList, jsb, "post_org_name");
			Utils.consStr(sqlItemList, jsb, "is_deleted");
			Utils.consInteger(sqlItemList, jsb, "create_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_created");
			Utils.consInteger(sqlItemList, jsb, "modify_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_modified");
			Utils.consStr(sqlItemList, jsb, "modify_user_name");
			Utils.consStr(sqlItemList, jsb, "create_user_name");

			sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
			sqlItemList.add(DB.notNull("created_by", "王小贱"));
			if (sqlItemList.size() > 1) {
				if (Utils.validate(jsb) && sqlItemList.size() > 0) {
					ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_package_"+ESB_YEAR+"(", sqlItemList.toArray(), ")",
							DB.valuesQuestions());
				}
			}

		}
	}

	public  void pkp_waybill_billInsert(JSONArray pkp_waybill_billJsa, DbContext ctx) {
		for (int i = 0; i < pkp_waybill_billJsa.size(); i++) {
			List<SqlItem> sqlItemList = new ArrayList<SqlItem>();
			JSONObject jsb = pkp_waybill_billJsa.getJSONObject(i);

			Utils.consInteger(sqlItemList, jsb, "waybill_bill_id");
			Utils.consInteger(sqlItemList, jsb, "pkp_waybill_id");
			Utils.consStr(sqlItemList, jsb, "waybill_no");
			Utils.consDate(sqlItemList, jsb, "biz_occur_date");
			Utils.consInteger(sqlItemList, jsb, "post_org_id");
			Utils.consStr(sqlItemList, jsb, "post_org_no");
			Utils.consStr(sqlItemList, jsb, "org_drds_code");
			Utils.consStr(sqlItemList, jsb, "post_org_name");
			Utils.consDate(sqlItemList, jsb, "customer_favourable_effect_time");
			Utils.consStr(sqlItemList, jsb, "customer_preferential_type");
			Utils.consStr(sqlItemList, jsb, "customers_preferential_id");
			Utils.consDecimal(sqlItemList, jsb, "standard_rates_start");
			Utils.consDecimal(sqlItemList, jsb, "standard_rates_continue");
			Utils.consInteger(sqlItemList, jsb, "preferential_cumulative_startnum");
			Utils.consInteger(sqlItemList, jsb, "preferential_cumulative_endnum");
			Utils.consInteger(sqlItemList, jsb, "preferential_cumulative_num");
			Utils.consStr(sqlItemList, jsb, "is_deleted");
			Utils.consInteger(sqlItemList, jsb, "create_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_created");
			Utils.consInteger(sqlItemList, jsb, "modify_user_id");
			Utils.consDate(sqlItemList, jsb, "gmt_modified");
			Utils.consStr(sqlItemList, jsb, "modify_user_name");
			Utils.consStr(sqlItemList, jsb, "create_user_name");

			sqlItemList.add(DB.notNull("created_date,", format.format(new Date())));
			sqlItemList.add(DB.notNull("created_by", "王小贱"));
			if (sqlItemList.size() > 1) {
				if (Utils.validate(jsb) && sqlItemList.size() > 0) {
					ctx.exe("INSERT INTO sdi_jxyz_pkp_waybill_bill_"+ESB_YEAR+"(", sqlItemList.toArray(), ")",
							DB.valuesQuestions());
				}
			}

		}
	}

}
