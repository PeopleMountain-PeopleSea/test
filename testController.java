package com.forceclouds.ds.edi.controller.api;

import com.TiprayAPI.cpp.TiprayAPI;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.forceclouds.ds.edi.access.business.query.model.VersionRecordModel;
import com.forceclouds.ds.edi.access.business.table.mapper.DdiComDataProductLotnumberRelationMapper;
import com.forceclouds.ds.edi.access.business.table.model.DdiComDataProductLotnumberRelation;
import com.forceclouds.ds.edi.access.business.table.model.DdiComDataProductLotnumberRelationKey;
import com.forceclouds.ds.edi.access.business.table.model.DdiDealerInfo;
import com.forceclouds.ds.edi.access.business.table.model.DdiProductInfo;
import com.forceclouds.ds.edi.access.common.query.mapper.DdiLotNumManageMapper;
import com.forceclouds.ds.edi.access.business.query.model.DdiCommonForUpload;
import com.forceclouds.ds.edi.base.annotation.Authorization;
import com.forceclouds.ds.edi.base.annotation.CurrentUser;
import com.forceclouds.ds.edi.base.model.UserInfoMode;
import com.forceclouds.ds.edi.service.ChangeService;
import com.forceclouds.ds.edi.service.impl.VersionRecordService;
import com.forceclouds.ds.edi.util.*;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.github.pagehelper.util.StringUtil;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.*;
import java.net.URLEncoder;
import java.util.*;

@RestController
@RequestMapping("/api/lotNum")
public class LotNumManageApiController {
    private static Logger logger = LogManager.getLogger(LotNumManageApiController.class.getName());
//    ForSwitchDb forSwitchDb = new ForSwitchDb();

    @Autowired
    private DdiLotNumManageMapper ddiLotNumManageMapper;

    @Autowired
    private DdiComDataProductLotnumberRelationMapper ddiComDataProductLotnumberRelationMapper;

    @Autowired
    private VersionRecordService versionRecordService;//20200422 Hazard 增加操作日志（修改，删除）

    @Value("${ddi.fileDownloadPath}")
    private String fileDownloadPath;

    @Value("${ddi.fileuploadPath}")
    private String fileuploadPath;

    //20240101 文件加密
    @Value("${ddi.useFileEncrypt}")
    private boolean useFileEncrypt;

    CommonUtils commonUtils = new CommonUtils();

    /**
     * @Description: 批号查询
     * @Author: Mr.Wang
     * @Date: 9:44 2018/6/8
     */
    @RequestMapping("/searchLotNumInfo")
    @Authorization
    public Map<String, Object> searchLotNumInfo(@RequestBody String json, @CurrentUser UserInfoMode loginUserInfo) {
        Map<String, Object> map = new HashMap<>();
        try {
            JSONObject object = JSON.parseObject(json);
//            forSwitchDb.switchDb(SysFix.DATA_SOURCE_COMMON);
            String pageSize = object.getString("rows");
            String nextPage = object.getString("page");
            String lotNumber = object.getString("lotNumber");
            String productName = object.getString("productName");
            String productCode = object.getString("productCode");
            String dealerName = object.getString("dealerName");
            //20200619 Hazard 批号管理增加有效期（可选择区间）查询 START
            String validityDay = object.getString("validityDay");
            String validityDayStart = "";
            String validityDayEnd = "";
            if (!StringUtils.isEmpty(validityDay)) {
                validityDayStart = validityDay.split(" - ")[0] + " 00:00:00";
                validityDayEnd = validityDay.split(" - ")[1] + " 23:59:59";
            }
            //20200619 Hazard 批号管理增加有效期（可选择区间）查询 END
            //20200506 Hazard 查询增加“创建人/时间，更新人/时间” START
            String insertUserName = object.getString("insertUserName");
            String insertDate = object.getString("insertDate");
            String startInsertDate = "";
            String endInsertDate = "";
            if (insertDate != null) {
                if (!insertDate.equals("")) {
                    startInsertDate = insertDate.split(" - ")[0] + " 00:00:00";
                    endInsertDate = insertDate.split(" - ")[1] + " 23:59:59";
                }
            } else {
                startInsertDate = "";
                endInsertDate = "";
            }

            String updateUserName = object.getString("updateUserName");
            String updateDate = object.getString("updateDate");
            String startUpdateDate = "";
            String endUpdateDate = "";
            if (updateDate != null) {
                if (!updateDate.equals("")) {
                    startUpdateDate = updateDate.split(" - ")[0] + " 00:00:00";
                    endUpdateDate = updateDate.split(" - ")[1] + " 23:59:59";
                }
            } else {
                startUpdateDate = "";
                endUpdateDate = "";
            }
            //20200506 Hazard 查询增加“创建人/时间，更新人/时间” END
            if (StringUtil.isEmpty(object.getString("sidx"))) {
                PageHelper.startPage(Integer.valueOf(nextPage), Integer.valueOf(pageSize));
            } else {
                PageHelper.startPage(Integer.valueOf(nextPage), Integer.valueOf(pageSize), object.getString("sidx") + " " + object.getString("sord"));
            }
            List<DdiCommonForUpload> list = ddiLotNumManageMapper.queryLotNum(lotNumber, productName, productCode, dealerName
                    , validityDayStart, validityDayEnd //20200619 Hazard 批号管理增加有效期（可选择区间）查询
                    , insertUserName, startInsertDate, endInsertDate, updateUserName, startUpdateDate, endUpdateDate //20200506 Hazard 查询增加“创建人/时间，更新人/时间”
            );
            PageInfo<DdiCommonForUpload> pageInfo = new PageInfo<>(list);
            long totalPages = pageInfo.getPages();
            long currPage = pageInfo.getPageNum();
            long totalCount = pageInfo.getTotal();
            if (!list.isEmpty()) {
                map.put("totalPages", totalPages);
                map.put("currPage", currPage);
                map.put("totalCount", totalCount);
                map.put("success", true);
                map.put("list", list);
            } else {
                map.put("success", false);
                map.put("msg", "无");
            }
        } catch (Exception e) {
            e.printStackTrace();
            map.put("success", false);
            map.put("msg", "系统错误");
        } finally {
//            forSwitchDb.cleanDb();
            return map;
        }
    }

}
