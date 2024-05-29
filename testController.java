package com.xunxi.bayer.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.csvreader.CsvWriter;
import com.xunxi.bayer.domain.constant.ResponseConstant;
import com.xunxi.bayer.domain.constant.UserConstant;
import com.xunxi.bayer.domain.entity.*;
import com.xunxi.bayer.domain.model.CustomerPostModel;
import com.xunxi.bayer.domain.model.UploadItemExplainModel;
import com.xunxi.bayer.mapper.*;
import com.xunxi.bayer.service.CuspostCommonService;
import com.xunxi.bayer.task.AddHubTask;
import com.xunxi.bayer.utils.*;
import com.xunxi.bayer.wrapper.Wrapper;
import io.swagger.annotations.ApiOperation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @MethodName 客岗关系调整
 * @Authror Hazard
 * @Date 2022/9/15 19:10
 */
@RestController
@RequestMapping(value = "/cusPost")
public class CustomerPostController {
    private static Logger logger = LogManager.getLogger(CustomerPostController.class);

    CommonUtils commonUtils = new CommonUtils();

    UploadUtils uploadUtils = new UploadUtils();

    CustomerPostUtils customerPostUtils = new CustomerPostUtils();

    @Value("${path.cusPostFileUploadPath}")
    private String cusPostFileUploadPath;

    @Value("${path.cusPostFileDownloadPath}")
    private String cusPostFileDownloadPath;

    // 下载文档用临时文件夹
    @Value("${path.cusPostTemporaryPath}")
    private String cusPostTemporaryPath;

    // 错误文件数据文件夹
    @Value("${path.cusPostErrorfilePath}")
    private String cusPostErrorfilePath;

    @Resource
    private MasterCommonMapper masterCommonMapper;

    @Resource
    private CustomerPostMapper customerPostMapper;

    @Resource
    private CuspostCommonService cuspostCommonService;

    @Resource
    private CuspostQuarterAdjustInfoMapper cuspostQuarterAdjustInfoMapper;

    @Resource
    private CuspostQuarterRetailAddDsmMapper cuspostQuarterRetailAddDsmMapper;

    @Resource
    private CuspostQuarterRetailChangeDsmMapper cuspostQuarterRetailChangeDsmMapper;

    @Resource
    private CustomerPostExcelUploadUtils customerPostExcelUploadUtils;

    @Resource
    private CuspostQuarterDataUploadInfoMapper cuspostQuarterDataUploadInfoMapper;

//    @Resource
//    private CuspostQuarterApplyCodeInfoMapper cuspostQuarterApplyCodeInfoMapper;

    @Resource
    private CuspostQuarterApplyStateInfoMapper cuspostQuarterApplyStateInfoMapper;

    @Resource
    private CuspostQuarterApplyStateRegionInfoMapper cuspostQuarterApplyStateRegionInfoMapper;

    @Resource
    private CuspostQuarterRetailChangeAssistantMapper cuspostQuarterRetailChangeAssistantMapper;

    @Resource
    private CuspostQuarterRetailAddAssistantMapper cuspostQuarterRetailAddAssistantMapper;

    @Resource
    private CuspostQuarterRetailAddDaMapper cuspostQuarterRetailAddDaMapper;

    @Resource
    private CuspostQuarterRetailChangeDaMapper cuspostQuarterRetailChangeDaMapper;

    @Resource
    private CuspostQuarterHospitalAddDsmMapper cuspostQuarterHospitalAddDsmMapper;

    @Resource
    private CuspostQuarterHospitalAddAssistantMapper cuspostQuarterHospitalAddAssistantMapper;

    @Resource
    private CuspostQuarterDistributorAddDsmMapper cuspostQuarterDistributorAddDsmMapper;

    @Resource
    private CuspostQuarterDistributorAddAssistantMapper cuspostQuarterDistributorAddAssistantMapper;

    @Resource
    private CuspostQuarterChainstoreHqAddDsmMapper cuspostQuarterChainstoreHqAddDsmMapper;

    @Resource
    private CuspostQuarterChainstoreHqAddAssistantMapper cuspostQuarterChainstoreHqAddAssistantMapper;

    @Resource
    private CuspostQuarterHospitalChangeDsmMapper cuspostQuarterHospitalChangeDsmMapper;

    @Resource
    private CuspostQuarterHospitalChangeAssistantMapper cuspostQuarterHospitalChangeAssistantMapper;

    @Resource
    private CuspostQuarterHospitalChangeDaMapper cuspostQuarterHospitalChangeDaMapper;

    @Resource
    private CuspostQuarterDistributorChangeDsmMapper cuspostQuarterDistributorChangeDsmMapper;

    @Resource
    private CuspostQuarterDistributorChangeAssistantMapper cuspostQuarterDistributorChangeAssistantMapper;

    @Resource
    private CuspostQuarterDistributorChangeDaMapper cuspostQuarterDistributorChangeDaMapper;

    @Resource
    private CuspostQuarterChainstoreHqChangeDsmMapper cuspostQuarterChainstoreHqChangeDsmMapper;

    @Resource
    private CuspostQuarterChainstoreHqChangeAssistantMapper cuspostQuarterChainstoreHqChangeAssistantMapper;

    @Resource
    private CuspostQuarterChainstoreHqChangeDaMapper cuspostQuarterChainstoreHqChangeDaMapper;

    @Resource
    private CuspostQuarterHospitalAddDaMapper cuspostQuarterHospitalAddDaMapper;

    @Resource
    private CuspostQuarterDistributorAddDaMapper cuspostQuarterDistributorAddDaMapper;

    @Resource
    private CuspostQuarterChainstoreHqAddDaMapper cuspostQuarterChainstoreHqAddDaMapper;

    @Resource
    private CuspostQuarterThirdPartyAreaMapper cuspostQuarterThirdPartyAreaMapper;

    @Resource
    private AddHubTask addHubTask;

    /*************************************************共通**************************************************************/
    /*********************************************获取验真数据****************************************************/

    /**
     * 获取地区经理（申请人）List
     */
    @ApiOperation(value = "获取地区经理（申请人）List", notes = "获取地区经理（申请人）List")
    @RequestMapping(value = "/queryDsm", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryDsm(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String lvl2Code = object.getString("region"); // region

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, "2", loginUser.getUserCode());
//            String lvl2Code = "";
//            if (lvlList.size() > 0) {
//                lvl2Code = lvlList.get(0).getLvl2Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            List<Map<String, String>> list = customerPostMapper.queryDsm(manageYear, manageQuarter, nowYM, lvl2Code);
//            List<Map<String, String>> list = customerPostMapper.queryDsm(manageYear, manageQuarter, nowYM, lvl2Codes);
            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /*********************************************客岗关系季度调整任务****************************************************/
    //region 客岗关系季度调整任务

    /**
     * 查询客岗关系季度调整任务
     */
    @ApiOperation(value = "查询客岗关系季度调整任务", notes = "查询客岗关系季度调整任务")
    @RequestMapping(value = "/queryCuspostAdjustQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCuspostAdjustQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String shutUpShopCode = object.getString("shutUpShopCode"); // 终端关店数据
            String hospitalPerCode = object.getString("hospitalPerCode"); // 医院业绩数据
            String retailPerCode = object.getString("retailPerCode"); // 零售终端业绩数据
            String chainstoreHqPerCode = object.getString("chainstoreHqPerCode"); // 连锁总部业绩数据
            String distributorPerCode = object.getString("distributorPerCode"); // 打单商业绩数据
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryCuspostAdjustQuarterInfo(page
                    , manageYear, manageQuarter, shutUpShopCode, hospitalPerCode
                    , retailPerCode, chainstoreHqPerCode, distributorPerCode
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    //新增前获取4个信息

    /**
     * 查询核心表的最新时间
     */
    @ApiOperation(value = "查询核心表的最新时间", notes = "查询核心表的最新时间")
    @RequestMapping(value = "/queryCoreTableMaxMonth", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCoreTableMaxMonth(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);

            //核心品牌-医院		hub_hco_hospital
            String hospitalMaxMonth = customerPostMapper.selectMaxManageMonth("hub_hco_hospital");
            //核心品牌-零售药店	hub_hco_retail
            String retailMaxMonth = customerPostMapper.selectMaxManageMonth("hub_hco_retail");
            //核心品牌-商业		hub_hco_distributor
            String distributorMaxMonth = customerPostMapper.selectMaxManageMonth("hub_hco_distributor");
            //核心品牌-连锁总部	hub_hco_chainstore_hq
            String chainstoreHqMaxMonth = customerPostMapper.selectMaxManageMonth("hub_hco_chainstore_hq");

            resultMap.put("hospitalMaxMonth", hospitalMaxMonth);
            resultMap.put("retailMaxMonth", retailMaxMonth);
            resultMap.put("distributorMaxMonth", distributorMaxMonth);
            resultMap.put("chainstoreHqMaxMonth", chainstoreHqMaxMonth);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 季度零售终端数据无更新（新增，修改删除通用）
     */
    @ApiOperation(value = "季度零售终端数据无更新（新增，修改删除通用）", notes = "季度零售终端数据无更新（新增，修改删除通用）")
    @RequestMapping(value = "/noUpdateQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper noUpdateQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String nowYM = commonUtils.getTodayYM2();
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, "1", loginUser.getUserCode());
            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
            String lvl4Code = "";
            if (lvlList.size() > 0) {
                lvl4Code = lvlList.get(0).getLvl4Code();
            } else {
                //架构错误
            }

            //更新申请编码状态 cuspost_quarter_apply_state_info
            CuspostQuarterApplyStateInfo cuspostQuarterApplyStateInfo = new CuspostQuarterApplyStateInfo();
            UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper = new UpdateWrapper<>();
            updateWrapper.set("retailApplyStateCode", UserConstant.APPLY_STATE_CODE_99);
            updateWrapper.eq("typeCode", typeCode);
            updateWrapper.eq("manageYear", manageYear);
            updateWrapper.eq("manageQuarter", manageQuarter);
            updateWrapper.eq("lvl4Code", lvl4Code);
            int insertCount = cuspostQuarterApplyStateInfoMapper.update(cuspostQuarterApplyStateInfo, updateWrapper);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /******************************************************************************************共通***************************************************************************************************/

    /**
     * 新增客岗关系季度调整任务
     */
    @ApiOperation(value = "新增客岗关系季度调整任务", notes = "新增客岗关系季度调整任务")
    @RequestMapping(value = "/addCuspostAdjustQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper addCuspostAdjustQuarterInfo(@RequestBody String json) {

        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        String userCode = loginUser.getUserCode();
        String _uuid = commonUtils.createUUID();

        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
//            int hospitalMaxMonth = object.getInteger("hospitalMaxMonth"); // 医院最新月份
//            int retailMaxMonth = object.getInteger("retailMaxMonth"); // 零售终端最新月份
//            int distributorMaxMonth = object.getInteger("distributorMaxMonth"); // 商业/打单商月份
//            int chainstoreHqMaxMonth = object.getInteger("chainstoreHqMaxMonth"); // 连锁总部最新月份

//            String hospitalMaxMonthStr = StringUtils.isEmpty(object.getString("hospitalMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_hospital") : object.getString("hospitalMaxMonth"); // 医院最新月份
//            String retailMaxMonthStr = StringUtils.isEmpty(object.getString("retailMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_retail") : object.getString("retailMaxMonth"); // 零售终端最新月份
//            String distributorMaxMonthStr = StringUtils.isEmpty(object.getString("distributorMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_distributor") : object.getString("distributorMaxMonth"); // 商业/打单商月份
//            String chainstoreHqMaxMonthStr = StringUtils.isEmpty(object.getString("chainstoreHqMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_chainstore_hq") : object.getString("chainstoreHqMaxMonth");; // 连锁总部最新月份

            // 20231110 避免查出结果不一致（慢或者页面停留）
            String hospitalMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_hospital"); // 医院最新月份
            String retailMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_retail"); // 零售终端最新月份
            String distributorMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_distributor"); // 商业/打单商月份
            String chainstoreHqMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_chainstore_hq");
            ; // 连锁总部最新月份

            int hospitalMaxMonth = Integer.parseInt(hospitalMaxMonthStr); // 医院最新月份
            int retailMaxMonth = Integer.parseInt(retailMaxMonthStr); // 零售终端最新月份
            int distributorMaxMonth = Integer.parseInt(distributorMaxMonthStr); // 商业/打单商月份
            int chainstoreHqMaxMonth = Integer.parseInt(chainstoreHqMaxMonthStr); // 连锁总部最新月份

            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String hospitalCheckBox = object.getString("hospitalCheckBox"); // 医院CheckBox
            String retailCheckBox = object.getString("retailCheckBox"); // 零售终端CheckBox
            String distributorCheckBox = object.getString("distributorCheckBox"); // 打单商CheckBox
            String chainstoreHqCheckBox = object.getString("chainstoreHqCheckBox"); // 连锁总部CheckBox

            String nowYM = commonUtils.getTodayYM2();


            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostCheck = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (!StringUtils.isEmpty(cuspostCheck)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "同一年度季度的数据已经存在");
            }

            if ("0".equals(hospitalCheckBox) && "0".equals(retailCheckBox) && "0".equals(distributorCheckBox) && "0".equals(chainstoreHqCheckBox)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "请勾选后创建");
            }

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //3.6.1 D&A在当季内任一时间，可选择创建下一季度的客户调整，创建时，系统自动复制该季度最新月份的客户数据并生成下一季度第一个月的客户数据。
            /**创建需要用到的数据*/
            //核心品牌-医院		hub_hco_hospital
            if ("1".equals(hospitalCheckBox)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertHospitalByTable(hospitalMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_hospital", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_hospital", manageMonth, 1);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_hospital", manageMonth); //20230706
                int flag3 = customerPostMapper.insertHospitalTempByTable("cuspost_quarter_hco_hospital_" + manageMonth, hospitalMaxMonth, manageMonth, _uuid, userCode);
            }
            //核心品牌-零售药店	hub_hco_retail
            if ("1".equals(retailCheckBox)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertRetailByTable(retailMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_retail", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_retail", manageMonth, 2);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_retail", manageMonth); //20230706
                int flag3 = customerPostMapper.insertRetailTempByTable("cuspost_quarter_hco_retail_" + manageMonth, retailMaxMonth, manageMonth, _uuid, userCode);
            }
            //核心品牌-商业		hub_hco_distributor
            if ("1".equals(distributorCheckBox)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertDistributorByTable(distributorMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_distributor", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_distributor", manageMonth, 3);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_distributor", manageMonth); //20230706
                int flag3 = customerPostMapper.insertDistributorTempByTable("cuspost_quarter_hco_distributor_" + manageMonth, distributorMaxMonth, manageMonth, _uuid, userCode);
            }
            //核心品牌-连锁总部	hub_hco_chainstore_hq
            if ("1".equals(chainstoreHqCheckBox)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertChainstoreHqByTable(chainstoreHqMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_chainstore_hq", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_chainstore_hq", manageMonth, 4);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_chainstore_hq", manageMonth); //20230706
                int flag3 = customerPostMapper.insertChainstoreHqTempByTable("cuspost_quarter_hco_chainstore_hq_" + manageMonth, chainstoreHqMaxMonth, manageMonth, _uuid, userCode);
            }

            /**新增终端任务 cuspost_quarter_adjust_info*/
            cuspostQuarterAdjustInfoMapper.delete(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            ); //20230706
            CuspostQuarterAdjustInfo cuspost = new CuspostQuarterAdjustInfo();
            cuspost.setManageYear(BigDecimal.valueOf(manageYear));
            cuspost.setManageQuarter(manageQuarter);
            cuspost.setShutUpShopCode("0");
            cuspost.setHospitalCheckBox(hospitalCheckBox);
            cuspost.setHospitalPerCode("0");
            cuspost.setRetailCheckBox(retailCheckBox);
            cuspost.setRetailPerCode("0");
            cuspost.setDistributorCheckBox(distributorCheckBox);
            cuspost.setDistributorPerCode("0");
            cuspost.setChainstoreHqCheckBox(chainstoreHqCheckBox);
            cuspost.setChainstoreHqPerCode("0");
            int flag = cuspostQuarterAdjustInfoMapper.insert(cuspost);

            /**创建下个季度整体的申请状态，大区-地区+4个状态码 cuspost_quarter_apply_state_info*/
            customerPostMapper.deleteApplyInitStateQuarterInfoByTon(manageYear, manageQuarter); //20230706
            customerPostMapper.insertApplyInitStateQuarterInfoByTon("1", manageYear, manageQuarter, nowYM
                    , "0" + hospitalCheckBox, "0" + retailCheckBox, "0" + distributorCheckBox, "0" + chainstoreHqCheckBox
                    , userCode);
            customerPostMapper.insertApplyInitStateQuarterInfoByTon("2", manageYear, manageQuarter, nowYM
                    , "0" + hospitalCheckBox, "0" + retailCheckBox, "0" + distributorCheckBox, "0" + chainstoreHqCheckBox
                    , userCode);

            /**创建申请编码 cuspost_quarter_apply_code_info*/
            // 20230424 新增申请编码
//            CuspostQuarterApplyCodeInfo applyCodeQuarterInfo = new CuspostQuarterApplyCodeInfo();
//            applyCodeQuarterInfo.setManageYear(BigDecimal.valueOf(manageYear));
//            applyCodeQuarterInfo.setManageQuarter(manageQuarter);
//            applyCodeQuarterInfo.setApplyCode(0);
//            cuspostQuarterApplyCodeInfoMapper.insert(applyCodeQuarterInfo);

            //20231113 异步4个核心表的未分配数据
            addHubTask.taskAddFourHubNotAssigned("新建", manageYear, manageQuarter, manageMonth, _uuid, userCode
                    , hospitalCheckBox, retailCheckBox, distributorCheckBox, chainstoreHqCheckBox
                    , null, null, null, null
                    , hospitalMaxMonth, retailMaxMonth, distributorMaxMonth, chainstoreHqMaxMonth);

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }


    /**
     * 修改客岗关系季度调整任务
     */
    @ApiOperation(value = "修改客岗关系季度调整任务", notes = "修改客岗关系季度调整任务")
    @RequestMapping(value = "/modifyCuspostAdjustQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper modifyCuspostAdjustQuarterInfo(@RequestBody String json) {

        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        String userCode = loginUser.getUserCode();
        String _uuid = commonUtils.createUUID();

        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            if (StringUtils.isEmpty(object.getString("autoKey"))) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "修改的场合主键没有值");
            }
            int autoKey = object.getInteger("autoKey"); // autoKey

//            int hospitalMaxMonth = object.getInteger("hospitalMaxMonth"); // 医院最新月份
//            int retailMaxMonth = object.getInteger("retailMaxMonth"); // 零售终端最新月份
//            int distributorMaxMonth = object.getInteger("distributorMaxMonth"); // 商业/打单商月份
//            int chainstoreHqMaxMonth = object.getInteger("chainstoreHqMaxMonth"); // 连锁总部最新月份
//            String hospitalMaxMonthStr = StringUtils.isEmpty(object.getString("hospitalMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_hospital") : object.getString("hospitalMaxMonth"); // 医院最新月份
//            String retailMaxMonthStr = StringUtils.isEmpty(object.getString("retailMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_retail") : object.getString("retailMaxMonth"); // 零售终端最新月份
//            String distributorMaxMonthStr = StringUtils.isEmpty(object.getString("distributorMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_distributor") : object.getString("distributorMaxMonth"); // 商业/打单商月份
//            String chainstoreHqMaxMonthStr = StringUtils.isEmpty(object.getString("chainstoreHqMaxMonth")) ? customerPostMapper.selectMaxManageMonth("hub_hco_chainstore_hq") : object.getString("chainstoreHqMaxMonth");; // 连锁总部最新月份

            // 20231110 避免查出结果不一致（慢或者页面停留）
            String hospitalMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_hospital"); // 医院最新月份
            String retailMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_retail"); // 零售终端最新月份
            String distributorMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_distributor"); // 商业/打单商月份
            String chainstoreHqMaxMonthStr = customerPostMapper.selectMaxManageMonth("hub_hco_chainstore_hq");
            ; // 连锁总部最新月份

            int hospitalMaxMonth = Integer.parseInt(hospitalMaxMonthStr); // 医院最新月份
            int retailMaxMonth = Integer.parseInt(retailMaxMonthStr); // 零售终端最新月份
            int distributorMaxMonth = Integer.parseInt(distributorMaxMonthStr); // 商业/打单商月份
            int chainstoreHqMaxMonth = Integer.parseInt(chainstoreHqMaxMonthStr); // 连锁总部最新月份

            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String hospitalCheckBox = object.getString("hospitalCheckBox"); // 医院CheckBox
            String retailCheckBox = object.getString("retailCheckBox"); // 零售终端CheckBox
            String distributorCheckBox = object.getString("distributorCheckBox"); // 打单商CheckBox
            String chainstoreHqCheckBox = object.getString("chainstoreHqCheckBox"); // 连锁总部CheckBox


            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostCheck = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("autoKey", autoKey)
            );
            if (StringUtils.isEmpty(cuspostCheck)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "没有修改的数据");
            }

            String hospitalCheckBoxBefore = cuspostCheck.getHospitalCheckBox();
            String retailCheckBoxBefore = cuspostCheck.getRetailCheckBox();
            String distributorCheckBoxBefore = cuspostCheck.getDistributorCheckBox();
            String chainstoreHqCheckBoxBefore = cuspostCheck.getChainstoreHqCheckBox();

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //3.6.1 任务创建后，D&A可修改勾选的主数据范围，修改范围为可对未勾选项目改为勾选，但不能将已勾选项目改为不勾选。
            //核心品牌-医院		hub_hco_hospital
            if ("1".equals(hospitalCheckBox) && "0".equals(hospitalCheckBoxBefore)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertHospitalByTable(hospitalMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_hospital", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_hospital", manageMonth, 1);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_hospital", manageMonth); //20230706
                int flag3 = customerPostMapper.insertHospitalTempByTable("cuspost_quarter_hco_hospital_" + manageMonth, hospitalMaxMonth, manageMonth, _uuid, userCode);
                int flag4 = customerPostMapper.updateHospitalApplyInitStateQuarterInfo(manageYear, manageQuarter, userCode);
            }
            //核心品牌-零售药店	hub_hco_retail
            if ("1".equals(retailCheckBox) && "0".equals(retailCheckBoxBefore)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertRetailByTable(retailMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_retail", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_retail", manageMonth, 2);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_retail", manageMonth); //20230706
                int flag3 = customerPostMapper.insertRetailTempByTable("cuspost_quarter_hco_retail_" + manageMonth, retailMaxMonth, manageMonth, _uuid, userCode);
                int flag4 = customerPostMapper.updateRetailApplyInitStateQuarterInfo(manageYear, manageQuarter, userCode);
            }
            //核心品牌-商业		hub_hco_distributor
            if ("1".equals(distributorCheckBox) && "0".equals(distributorCheckBoxBefore)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertDistributorByTable(distributorMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_distributor", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_distributor", manageMonth, 3);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_distributor", manageMonth); //20230706
                int flag3 = customerPostMapper.insertDistributorTempByTable("cuspost_quarter_hco_distributor_" + manageMonth, distributorMaxMonth, manageMonth, _uuid, userCode);
                int flag4 = customerPostMapper.updateDistributorApplyInitStateQuarterInfo(manageYear, manageQuarter, userCode);
            }
            //核心品牌-连锁总部	hub_hco_chainstore_hq
            if ("1".equals(chainstoreHqCheckBox) && "0".equals(chainstoreHqCheckBoxBefore)) {
                //20230113 创建表的时候判断目标月数据是否存在，不存在再创建
//                int coreCount = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, null, null);

                //20231113 创建客岗关系季度调整任务不要 START
//                int coreCount = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, null, null, null, null);// 20230414 主数据中dsm无值，也可以进行新增
//                if (coreCount <= 0) {
//                    int flag1 = customerPostMapper.insertChainstoreHqByTable(chainstoreHqMaxMonth, manageMonth, _uuid, userCode);
//                }
                //20231113 创建客岗关系季度调整任务不要 END

                //创建新表
                int flagDrop = customerPostMapper.dropCuspostTempTable("cuspost_quarter_hco_chainstore_hq", manageMonth); //20230705
                int flag2 = customerPostMapper.createCuspostTempTable("cuspost_quarter_hco_chainstore_hq", manageMonth, 4);
                int flagAlter = customerPostMapper.alterCuspostTempTable("cuspost_quarter_hco_chainstore_hq", manageMonth); //20230706
                int flag3 = customerPostMapper.insertChainstoreHqTempByTable("cuspost_quarter_hco_chainstore_hq_" + manageMonth, chainstoreHqMaxMonth, manageMonth, _uuid, userCode);
                int flag4 = customerPostMapper.updateChainstoreHqApplyInitStateQuarterInfo(manageYear, manageQuarter, userCode);
            }

            //修改终端任务
            CuspostQuarterAdjustInfo cuspostQuarterAdjustInfo = new CuspostQuarterAdjustInfo();
            UpdateWrapper<CuspostQuarterAdjustInfo> updateWrapper = new UpdateWrapper<>();
            updateWrapper.set("hospitalCheckBox", hospitalCheckBox);
            updateWrapper.set("retailCheckBox", retailCheckBox);
            updateWrapper.set("distributorCheckBox", distributorCheckBox);
            updateWrapper.set("chainstoreHqCheckBox", chainstoreHqCheckBox);
            updateWrapper.eq("autoKey", autoKey);
            int insertCount = cuspostQuarterAdjustInfoMapper.update(cuspostQuarterAdjustInfo, updateWrapper);

            //20231113 异步4个核心表的未分配数据
            addHubTask.taskAddFourHubNotAssigned("修改", manageYear, manageQuarter, manageMonth, _uuid, userCode
                    , hospitalCheckBox, retailCheckBox, distributorCheckBox, chainstoreHqCheckBox
                    , hospitalCheckBoxBefore, retailCheckBoxBefore, distributorCheckBoxBefore, chainstoreHqCheckBoxBefore
                    , hospitalMaxMonth, retailMaxMonth, distributorMaxMonth, chainstoreHqMaxMonth);

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    //endregion

    /*********************************************终端关店数据管理****************************************************/

    //region 终端关店数据管理
    //核心品牌-零售药店	hub_hco_retail

    /**
     * 查询季度终端关店数据
     */
    @ApiOperation(value = "查询季度终端关店数据", notes = "查询季度终端关店数据")
    @RequestMapping(value = "/queryCuspostShutupShopQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCuspostShutupShopQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String shutupShopRemark = object.getString("shutupShopRemark"); // 关店备注
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryCuspostShutupShopQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName, shutupShopRemark
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }


    /**
     * @MethodName 上传季度终端关店数据
     * @Authror Hazard
     * @Date 2022/9/17 13:30
     */
    //上传同时更新客岗任务主表
    @ApiOperation(value = "上传季度终端关店数据", notes = "上传季度终端关店数据")
    @RequestMapping("/batchAddCuspostShutupShopQuarterInfo")
    @Transactional
    public Wrapper batchAddCuspostShutupShopQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //相关数据正在导入，预计15分钟完成，请等待完成后再导入关店数据
            int coreCount = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, null, null, null);
            if (coreCount <= 0) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "相关数据正在导入，预计20分钟完成，请等待完成后再导入关店数据！");
            }

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.SHUT_UP_SHOP_QUARTER_INFO);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = shutupShopQuarterDataBatch("cuspost_quarter_shutup_shop_info", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //更新终端关店数据在任务数据中的状态
                    //修改终端任务,终端关店数据-已上传
                    CuspostQuarterAdjustInfo cuspostQuarterAdjustInfo = new CuspostQuarterAdjustInfo();
                    UpdateWrapper<CuspostQuarterAdjustInfo> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("shutUpShopCode", "1");
                    updateWrapper.eq("autoKey", cuspostInfo.getAutoKey());
                    int insertCount = cuspostQuarterAdjustInfoMapper.update(cuspostQuarterAdjustInfo, updateWrapper);

                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String shutupShopQuarterDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, manageMonth);

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //20231113 START
                //删除调整类型
                customerPostMapper.deleteCuspostHcoRetailAdjustType(manageMonth);
                //删除零售变更删除DSM申请数据
                customerPostMapper.deleteQuarterRetailChangeDsmInsert(manageMonth);
                // 更新上传数据
                customerPostMapper.deleteShutupShop(manageMonth);
                //20231113 END

                //更新年度，季度
                customerPostMapper.uploadShutupShopYearQuarter(fileId, manageYear, manageQuarter, manageMonth);
                //更新cuspost_quarter_hco_retail_yyyymm的调整类型为删除
                customerPostMapper.updateCuspostHcoRetailAdjustType(fileId, manageMonth);
                // 20230606 插入零售变更删除DSM申请数据 不能放在下面
                customerPostMapper.uploadQuarterRetailChangeDsmInsert(fileId, manageMonth, userCode);
                // 更新上传数据
//                customerPostMapper.uploadShutupShopUpdate(fileId, userCode); //20231113 不更新操作
                // 插入上传数据
                customerPostMapper.uploadShutupShopInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * @MethodName 下载季度终端关店数据
     * @Authror Hazard
     * @Date 2022/9/16 23:10
     */
    @ApiOperation(value = "下载季度终端关店数据", notes = "下载季度终端关店数据")
    @RequestMapping(value = "/exprotCuspostShutupShopQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotCuspostShutupShopQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String shutupShopRemark = object.getString("shutupShopRemark"); // 关店备注
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> shutupShopQuarterInfo = customerPostMapper.queryCuspostShutupShopQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName, shutupShopRemark
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.SHUT_UP_SHOP_QUARTER_INFO);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度终端关店数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, shutupShopQuarterInfo.getRecords());

            // 下载压缩文件 downloadFileForZipWithDelete
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    //endregion

    /*********************************************业绩数据****************************************************/
    //region 业绩数据


    /**
     * 查询季度医院业绩数据
     */
    @ApiOperation(value = "查询季度医院业绩数据", notes = "查询季度医院业绩数据")
    @RequestMapping(value = "/queryCuspostHospitalPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCuspostHospitalPerQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryCuspostHospitalQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * @MethodName 上传季度医院业绩数据
     * @Authror Hazard
     * @Date 2022/9/17 13:30
     */
    //上传同时更新客岗任务主表
    @ApiOperation(value = "上传季度医院业绩数据", notes = "上传季度医院业绩数据")
    @RequestMapping("/batchAddCuspostHospitalPerQuarterInfo")
    @Transactional
    public Wrapper batchAddCuspostHospitalPerQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            int statisticStartMonth = Integer.parseInt(request.getParameter("statisticStartMonth")); //统计开始月份
            int statisticEndMonth = Integer.parseInt(request.getParameter("statisticEndMonth")); //统计截止月份

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("hospitalCheckBox", "1")
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.HOSPITAL_PER_QUARTER_INFO);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = hospitalPerQuarterDataBatch("cuspost_quarter_hospital_per_info", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter
                        , statisticStartMonth, statisticEndMonth
                );

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //更新终端关店数据在任务数据中的状态
                    //修改终端任务,终端关店数据-已上传
                    CuspostQuarterAdjustInfo cuspostQuarterAdjustInfo = new CuspostQuarterAdjustInfo();
                    UpdateWrapper<CuspostQuarterAdjustInfo> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("hospitalPerCode", "1");
                    updateWrapper.eq("autoKey", cuspostInfo.getAutoKey());
                    int insertCount = cuspostQuarterAdjustInfoMapper.update(cuspostQuarterAdjustInfo, updateWrapper);

                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String hospitalPerQuarterDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter
            , int statisticStartMonth, int statisticEndMonth
    ) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, manageMonth);

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
                customerPostMapper.uploadHospitalPerYearQuarter(fileId, manageYear, manageQuarter, manageMonth, statisticStartMonth, statisticEndMonth);
                // 更新上传数据
                customerPostMapper.uploadHospitalPerUpdate(fileId, userCode);
                // 插入新数据
                customerPostMapper.uploadHospitalPerInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * @MethodName 下载季度医院业绩数据
     * @Authror Hazard
     * @Date 2022/9/16 23:10
     */
    @ApiOperation(value = "下载季度医院业绩数据", notes = "下载季度医院业绩数据")
    @RequestMapping(value = "/exprotCuspostHospitalPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotCuspostHospitalPerQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> shutupShopQuarterInfo = customerPostMapper.queryCuspostHospitalQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.HOSPITAL_PER_QUARTER_INFO);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院业绩数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, shutupShopQuarterInfo.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * 查询季度零售终端业绩数据
     */
    @ApiOperation(value = "查询季度零售终端业绩数据", notes = "查询季度零售终端业绩数据")
    @RequestMapping(value = "/queryCuspostRetailPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCuspostRetailPerQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryCuspostRetailQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }


    /**
     * 上传季度零售终端业绩数据
     */
    //上传同时更新客岗任务主表
    @ApiOperation(value = "上传季度零售终端业绩数据", notes = "上传季度零售终端业绩数据")
    @RequestMapping("/batchAddCuspostRetailPerQuarterInfo")
    @Transactional
    public Wrapper batchAddCuspostRetailPerQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            int statisticStartMonth = Integer.parseInt(request.getParameter("statisticStartMonth")); //统计开始月份
            int statisticEndMonth = Integer.parseInt(request.getParameter("statisticEndMonth")); //统计截止月份

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("retailCheckBox", "1")
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.RETAIL_PER_QUARTER_INFO);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = retailPerQuarterDataBatch("cuspost_quarter_retail_per_info", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter
                        , statisticStartMonth, statisticEndMonth
                );

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //更新终端关店数据在任务数据中的状态
                    //修改终端任务,终端关店数据-已上传
                    CuspostQuarterAdjustInfo cuspostQuarterAdjustInfo = new CuspostQuarterAdjustInfo();
                    UpdateWrapper<CuspostQuarterAdjustInfo> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("retailPerCode", "1");
                    updateWrapper.eq("autoKey", cuspostInfo.getAutoKey());
                    int insertCount = cuspostQuarterAdjustInfoMapper.update(cuspostQuarterAdjustInfo, updateWrapper);

                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String retailPerQuarterDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter
            , int statisticStartMonth, int statisticEndMonth
    ) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, manageMonth);

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
                customerPostMapper.uploadRetailPerYearQuarter(fileId, manageYear, manageQuarter, manageMonth, statisticStartMonth, statisticEndMonth);
                // 更新上传数据
                customerPostMapper.uploadRetailPerUpdate(fileId, userCode);
                // 插入上传数据
                customerPostMapper.uploadRetailPerInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }


    /**
     * 下载季度零售终端业绩数据
     */
    @ApiOperation(value = "下载季度零售终端业绩数据", notes = "下载季度零售终端业绩数据")
    @RequestMapping(value = "/exprotCuspostRetailPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotCuspostRetailPerQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> shutupShopQuarterInfo = customerPostMapper.queryCuspostRetailQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.RETAIL_PER_QUARTER_INFO);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度零售终端业绩数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, shutupShopQuarterInfo.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 查询季度商务打单商业绩数据
     */
    @ApiOperation(value = "查询季度商务打单商业绩数据", notes = "查询季度商务打单商业绩数据")
    @RequestMapping(value = "/queryCuspostDistributorPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCuspostDistributorPerQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryCuspostDistributorQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }


    /**
     * 上传季度商务打单商业绩数据
     */
    //上传同时更新客岗任务主表
    @ApiOperation(value = "上传季度商务打单商业绩数据", notes = "上传季度商务打单商业绩数据")
    @RequestMapping("/batchAddCuspostDistributorPerQuarterInfo")
    @Transactional
    public Wrapper batchAddCuspostDistributorPerQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            int statisticStartMonth = Integer.parseInt(request.getParameter("statisticStartMonth")); //统计开始月份
            int statisticEndMonth = Integer.parseInt(request.getParameter("statisticEndMonth")); //统计截止月份

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("distributorCheckBox", "1")
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.DISTRIBUTOR_PER_QUARTER_INFO);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = distributorPerQuarterDataBatch("cuspost_quarter_distributor_per_info", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter
                        , statisticStartMonth, statisticEndMonth
                );

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //更新终端关店数据在任务数据中的状态
                    //修改终端任务,终端关店数据-已上传
                    CuspostQuarterAdjustInfo cuspostQuarterAdjustInfo = new CuspostQuarterAdjustInfo();
                    UpdateWrapper<CuspostQuarterAdjustInfo> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("distributorPerCode", "1");
                    updateWrapper.eq("autoKey", cuspostInfo.getAutoKey());
                    int insertCount = cuspostQuarterAdjustInfoMapper.update(cuspostQuarterAdjustInfo, updateWrapper);

                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String distributorPerQuarterDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter
            , int statisticStartMonth, int statisticEndMonth
    ) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, manageMonth);

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
                customerPostMapper.uploadDistributorPerYearQuarter(fileId, manageYear, manageQuarter, manageMonth, statisticStartMonth, statisticEndMonth);
                // 更新上传数据
                customerPostMapper.uploadDistributorPerUpdate(fileId, userCode);
                // 插入新数据
                customerPostMapper.uploadDistributorPerInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }


    /**
     * 下载季度商务打单商业绩数据
     */
    @ApiOperation(value = "下载季度商务打单商业绩数据", notes = "下载季度商务打单商业绩数据")
    @RequestMapping(value = "/exprotCuspostDistributorPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotCuspostDistributorPerQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> shutupShopQuarterInfo = customerPostMapper.queryCuspostDistributorQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.DISTRIBUTOR_PER_QUARTER_INFO);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度商务打单商业绩数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, shutupShopQuarterInfo.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 查询季度连锁总部业绩数据
     */
    @ApiOperation(value = "查询季度连锁总部业绩数据", notes = "查询季度连锁总部业绩数据")
    @RequestMapping(value = "/queryCuspostChainstoreHqPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCuspostChainstoreHqPerQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryCuspostChainstoreHqQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }


    /**
     * 上传季度连锁总部业绩数据
     */
    //上传同时更新客岗任务主表
    @ApiOperation(value = "上传季度连锁总部业绩数据", notes = "上传季度连锁总部业绩数据")
    @RequestMapping("/batchAddCuspostChainstoreHqPerQuarterInfo")
    @Transactional
    public Wrapper batchAddCuspostChainstoreHqPerQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            int statisticStartMonth = Integer.parseInt(request.getParameter("statisticStartMonth")); //统计开始月份
            int statisticEndMonth = Integer.parseInt(request.getParameter("statisticEndMonth")); //统计截止月份

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("chainstoreHqCheckBox", "1")
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.CHAINSTORE_HQ_PER_QUARTER_INFO);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = chainstoreHqPerQuarterDataBatch("cuspost_quarter_chainstore_hq_per_info", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter
                        , statisticStartMonth, statisticEndMonth
                );

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //更新终端关店数据在任务数据中的状态
                    //修改终端任务,终端关店数据-已上传
                    CuspostQuarterAdjustInfo cuspostQuarterAdjustInfo = new CuspostQuarterAdjustInfo();
                    UpdateWrapper<CuspostQuarterAdjustInfo> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("chainstoreHqPerCode", "1");
                    updateWrapper.eq("autoKey", cuspostInfo.getAutoKey());
                    int insertCount = cuspostQuarterAdjustInfoMapper.update(cuspostQuarterAdjustInfo, updateWrapper);

                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String chainstoreHqPerQuarterDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter
            , int statisticStartMonth, int statisticEndMonth
    ) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, manageMonth);

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
                customerPostMapper.uploadChainstoreHqPerYearQuarter(fileId, manageYear, manageQuarter, manageMonth, statisticStartMonth, statisticEndMonth);
                // 更新上传数据
                customerPostMapper.uploadChainstoreHqPerUpdate(fileId, userCode);
                // 插入新数据
                customerPostMapper.uploadChainstoreHqPerInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }


    /**
     * 下载季度连锁总部业绩数据
     */
    @ApiOperation(value = "下载季度连锁总部业绩数据", notes = "下载季度连锁总部业绩数据")
    @RequestMapping(value = "/exprotCuspostChainstoreHqPerQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotCuspostChainstoreHqPerQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 终端编码
            String customerName = object.getString("customerName"); // 终端名称
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> shutupShopQuarterInfo = customerPostMapper.queryCuspostChainstoreHqQuarterInfo(page
                    , manageYear, manageQuarter, customerCode, customerName
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.CHAINSTORE_HQ_PER_QUARTER_INFO);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度连锁总部业绩数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, shutupShopQuarterInfo.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    //endregion

    /*********************************************季度客岗调整申请****************************************************/
    //region 季度客岗调整申请

    /**
     * 查询季度地区经理大区助理申请变更删除一览数据
     */
    @ApiOperation(value = "查询季度地区经理大区助理申请变更删除一览数据", notes = "查询季度地区经理大区助理申请变更删除一览数据")
    @RequestMapping(value = "/queryDsmApplyTotalQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryDsmApplyTotalQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String postCode = object.getString("postCode"); // 岗位编码，1：地区经理，2：大区助理

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage) || StringUtils.isEmpty(typeCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            //获取登录人详细信息
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();


            // 检索处理
            List<Map<String, Object>> list = new ArrayList<>();
            IPage<Map<String, Object>> result = null;
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            if (UserConstant.POST_CODE1.equals(postCode)) {
                result = customerPostMapper.queryDsmApplyTotalQuarterInfo(page, typeCode, manageYear, manageQuarter, userCode, nowYM);
            } else if (UserConstant.POST_CODE2.equals(postCode)) {
                result = customerPostMapper.queryAssistantApplyTotalQuarterInfo(page, typeCode, manageYear, manageQuarter, userCode, nowYM);
            }
            list = result.getRecords();


            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 查询季度大区助理提交后一览数据
     */
    @ApiOperation(value = "查询季度大区助理提交后一览数据", notes = "查询季度地区经理大区助理申请变更删除一览数据")
    @RequestMapping(value = "/queryRegionApplyTotalQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryRegionApplyTotalQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage) || StringUtils.isEmpty(typeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            //获取登录人详细信息
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();
            List<Map<String, Object>> list = new ArrayList<>();
            IPage<Map<String, Object>> result = null;
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);

            //大区经理（总监），渠道总监，BU_HEAD查询一览
            String postCode = null;//类型编码，3：大区经理，4：渠道总监，5：BuHead
            String region = null;//大区
//            String roleName = customerPostMapper.getRoleNameByUserCode(loginUser.getUserCode());
//            if ("商务总监".equals(roleName) || "大区总监".equals(roleName)) {
            if ("D917E598-0C23-4C26-979A-D8EF056EB336".equals(loginUser.getRoleCode())) { //20230206 大区总监
                postCode = "3";
                List<Map<String, String>> levelCodeList = customerPostMapper.queryRegionLevelCode(nowYM, loginUser.getUserCode());
                if (!levelCodeList.isEmpty()) {
                    region = levelCodeList.get(0).get("lvl2Code");
                    result = customerPostMapper.queryRegionApplyTotalQuarterInfo(page, typeCode, manageYear, manageQuarter, userCode, postCode, region);
                } else {
                    System.out.println();
                }

            }
//            if ("渠道总监".equals(roleName)) {
            if ("7AE962B2-3409-46A3-B6BC-307934F4C738".equals(loginUser.getRoleCode())) { //20230206 商务总监
                postCode = "4";
                result = customerPostMapper.queryRegionApplyTotalQuarterInfo(page, typeCode, manageYear, manageQuarter, userCode, postCode, null);
            }
//            if ("BuHead".equals(roleName)) {
            if ("265EE6A2-4156-4392-83AA-3C58AC9E3F95".equals(loginUser.getRoleCode())) { //20230206 渠道总监、BuHead
                postCode = "5";
                result = customerPostMapper.queryRegionApplyTotalQuarterInfo(page, typeCode, manageYear, manageQuarter, userCode, postCode, null);
            }
            if (StringUtils.isEmpty(result)) {

            } else {
                list = result.getRecords();
            }

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 查询季度医院申请数据
     */
    @ApiOperation(value = "查询季度医院申请数据", notes = "查询季度医院申请数据")
    @RequestMapping(value = "/queryHospitalApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryHospitalApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String postCode = object.getString("postCode"); // postCode
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            /**获取大区，地区等岗位编码*/
//            String lvl2Code = "";
//            String lvl4Code = "";
//            if (UserConstant.POST_CODE1.equals(postCode)) {
//
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode())
//                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
//                if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                    lvl4Code = lvlList.get(0).getLvl4Code();
//                } else {
//                    //架构错误
//                }
//            };


            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryHospitalApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, drugstoreProperty1Code
                    , region
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载季度医院数据(新增&变更)
     */
    @ApiOperation(value = "下载季度医院数据(新增&变更)", notes = "下载季度医院数据(新增&变更)")
    @RequestMapping(value = "/exprotHospitalAddChangeQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotHospitalAddChangeQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); //
            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> pageAdd = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultAdd = customerPostMapper.queryHospitalApplyQuarterInfo(pageAdd
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, null, null
                    , null, null, null
                    , region
                    , orderName //20230302 排序
            );

            Page<Map<String, Object>> pageChange = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultChange = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("hospitalCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                resultChange = customerPostMapper.queryHospitalChangeDeletionQuarterInfo(pageChange
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , Integer.parseInt(manageYear), manageQuarter, manageMonth, null, null, null, null
                        , null, null, null, null, null
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelListAdd = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_HOSPITAL_ADD);
            List<UploadItemExplainModel> downItemExplainModelListAdd = uploadItemExplainModelListAdd.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            List<UploadItemExplainModel> uploadItemExplainModelListChange = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_HOSPITAL_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelListChange = uploadItemExplainModelListChange.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院全部数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFileTwo(fileName, cusPostTemporaryPath, downItemExplainModelListAdd, resultAdd.getRecords()
                    , downItemExplainModelListChange, StringUtils.isEmpty(resultChange) ? null : resultChange.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载季度零售数据(新增&变更)
     */
    @ApiOperation(value = "下载季度零售数据(新增&变更)", notes = "下载季度零售数据(新增&变更)")
    @RequestMapping(value = "/exprotRetailAddChangeQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotRetailAddChangeQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); //
            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> pageAdd = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultAdd = customerPostMapper.queryRetailApplyQuarterInfo(pageAdd
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, null, null
                    , null, null, null
                    , region
                    , orderName //20230302 排序
            );

            Page<Map<String, Object>> pageChange = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultChange = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("retailCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                resultChange = customerPostMapper.queryRetailChangeDeletionQuarterInfo(pageChange
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , Integer.parseInt(manageYear), manageQuarter, manageMonth, null, null, null, null
                        , null, null, null, null, null
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelListAdd = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_RETAIL_ADD);
            List<UploadItemExplainModel> downItemExplainModelListAdd = uploadItemExplainModelListAdd.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            List<UploadItemExplainModel> uploadItemExplainModelListChange = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_RETAIL_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelListChange = uploadItemExplainModelListChange.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院全部数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFileTwo(fileName, cusPostTemporaryPath, downItemExplainModelListAdd, resultAdd.getRecords()
                    , downItemExplainModelListChange, StringUtils.isEmpty(resultChange) ? null : resultChange.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载季度商务数据(新增&变更)
     */
    @ApiOperation(value = "下载季度商务数据(新增&变更)", notes = "下载季度商务数据(新增&变更)")
    @RequestMapping(value = "/exprotDistributorAddChangeQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotDistributorAddChangeQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); //
            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> pageAdd = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultAdd = customerPostMapper.queryDistributorApplyQuarterInfo(pageAdd
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, null, null
                    , null, null, null
                    , region
                    , orderName //20230302 排序
            );

            Page<Map<String, Object>> pageChange = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultChange = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("distributorCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                resultChange = customerPostMapper.queryDistributorChangeDeletionQuarterInfo(pageChange
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , Integer.parseInt(manageYear), manageQuarter, manageMonth, null, null, null, null
                        , null, null, null, null, null
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelListAdd = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DISTRIBUTOR_ADD);
            List<UploadItemExplainModel> downItemExplainModelListAdd = uploadItemExplainModelListAdd.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            List<UploadItemExplainModel> uploadItemExplainModelListChange = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DISTRIBUTOR_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelListChange = uploadItemExplainModelListChange.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院全部数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFileTwo(fileName, cusPostTemporaryPath, downItemExplainModelListAdd, resultAdd.getRecords()
                    , downItemExplainModelListChange, StringUtils.isEmpty(resultChange) ? null : resultChange.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载季度连锁数据(新增&变更)
     */
    @ApiOperation(value = "下载季度连锁数据(新增&变更)", notes = "下载季度连锁数据(新增&变更)")
    @RequestMapping(value = "/exprotChainstoreHqAddChangeQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotChainstoreHqAddChangeQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); //
            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> pageAdd = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultAdd = customerPostMapper.queryChainstoreHqApplyQuarterInfo(pageAdd
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, null, null
                    , null, null
                    , region
                    , orderName //20230302 排序
            );

            Page<Map<String, Object>> pageChange = new Page<>(-1, -1);
            IPage<Map<String, Object>> resultChange = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("chainstoreHqCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                resultChange = customerPostMapper.queryChainstoreHqChangeDeletionQuarterInfo(pageChange
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , Integer.parseInt(manageYear), manageQuarter, manageMonth, null, null, null, null
                        , null, null, null, null, null
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelListAdd = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_CHAINSTORE_HQ_ADD);
            List<UploadItemExplainModel> downItemExplainModelListAdd = uploadItemExplainModelListAdd.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            List<UploadItemExplainModel> uploadItemExplainModelListChange = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_CHAINSTORE_HQ_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelListChange = uploadItemExplainModelListChange.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院全部数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFileTwo(fileName, cusPostTemporaryPath, downItemExplainModelListAdd, resultAdd.getRecords()
                    , downItemExplainModelListChange, StringUtils.isEmpty(resultChange) ? null : resultChange.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * 下载季度医院申请数据
     */
    @ApiOperation(value = "下载季度医院申请数据", notes = "下载季度医院申请数据")
    @RequestMapping(value = "/exprotHospitalApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotHospitalApplyQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryHospitalApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, drugstoreProperty1Code
                    , region
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_HOSPITAL_ADD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }



    /**
     * @MethodName 下载季度医院申请数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度医院申请数据 D&A下载按照上传模板顺序", notes = "下载季度医院申请数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotHospitalApplyQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotHospitalApplyQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryHospitalApplyQuarterInfo(page
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, drugstoreProperty1Code
                    , region
                    , orderName
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_HOSPITAL_APPLY_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 新增季度医院申请数据
     */
    @ApiOperation(value = "新增季度医院申请数据", notes = "新增季度医院申请数据")
    @RequestMapping(value = "/addHospitalApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper addHospitalApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                     // 省份
            String city = object.getString("city");                             // 城市
            String address = object.getString("address");                               // 地址
            String sameTimeRetailCode = object.getString("sameTimeRetailCode");                 // 同时为零售终端
            String otherPropertyCode = object.getString("otherPropertyCode");           // 其他属性
            String upHospitalCode = object.getString("upHospitalCode");                 // 上级医院CODE
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String repCode = object.getString("repCode");                               // REP岗位代码
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                   // postCode
            String region = object.getString("region");                   // region
            //20230529 START
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String upCustomerCode = object.getString("upCustomerCode");                   // 上级客户代码
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(sameTimeRetailCode) || StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(repCode)
                    || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }
            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_HOSPITAL);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_HOSPITAL);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }

            /**校验 架构城市关系*/
            int countFromStructureCity = customerPostMapper.queryCountFromStructureCity(nowYM, repCode, city);
            if (countFromStructureCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "架构城市关系错误", "架构城市关系不正确！");
            }

            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryHospitalDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, "");
            int count3 = customerPostMapper.queryHospitalDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, "");
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //获取repName,repCwid
            Map<String, String> repMap = customerPostMapper.getDataNameByDataCode(nowYM, repCode);
            String repName = null;
            String repCwid = null;
            if (!StringUtils.isEmpty(repMap)) {
                repName = repMap.get("userName");
                repCwid = repMap.get("cwid");
            } else {
                //架构错误
            }

            /**创建applyCode申请编码*/
//            String applyCodeStr = this.getApplyCode(manageYear, manageQuarter);
//            if (StringUtils.isEmpty(applyCodeStr)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "错误", "申请编码没有数据！");
//            }
            String applyCodeStr = commonUtils.createUUID().replaceAll("-", "");// 20230424 新增申请编码

            /**插入数据表*/
            if (UserConstant.POST_CODE1.equals(postCode)) {
                CuspostQuarterHospitalAddDsm info = new CuspostQuarterHospitalAddDsm();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("医院");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setSameTimeRetailCode(sameTimeRetailCode);
                info.setOtherPropertyCode(otherPropertyCode);
                info.setUpHospitalCode(upHospitalCode);
                info.setDsmCode(dsmCode);
                info.setDsmCwid(dsmCwid);
                info.setDsmName(dsmName);
                info.setRepCode(repCode);
                info.setRepCwid(repCwid);
                info.setRepName(repName);
                info.setDrugstoreProperty1Code(drugstoreProperty1Code);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTerritoryProducts(territoryProducts);
                info.setUpCustomerCode(upCustomerCode);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterHospitalAddDsmMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }

            if (UserConstant.POST_CODE2.equals(postCode)) {
                CuspostQuarterHospitalAddAssistant info = new CuspostQuarterHospitalAddAssistant();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("医院");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setSameTimeRetailCode(sameTimeRetailCode);
                info.setOtherPropertyCode(otherPropertyCode);
                info.setUpHospitalCode(upHospitalCode);
                info.setDsmCode(dsmCode);
                info.setDsmCwid(dsmCwid);
                info.setDsmName(dsmName);
                info.setRepCode(repCode);
                info.setRepCwid(repCwid);
                info.setRepName(repName);
                info.setDrugstoreProperty1Code(drugstoreProperty1Code);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTerritoryProducts(territoryProducts);
                info.setUpCustomerCode(upCustomerCode);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterHospitalAddAssistantMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 更新季度医院申请数据
     */
    @Transactional
    @ApiOperation(value = "修改季度医院申请数据", notes = "更新季度医院申请数据")
    @RequestMapping(value = "/updateHospitalApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper updateHospitalApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int autoKey = object.getInteger("autoKey");                                   // autoKey
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String applyCode = object.getString("applyCode");                  // 申请编码
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                     // 省份
            String city = object.getString("city");                             // 城市
            String address = object.getString("address");                               // 地址
            String sameTimeRetailCode = object.getString("sameTimeRetailCode");         // 同时为零售终端
            String otherPropertyCode = object.getString("otherPropertyCode");           // 其他属性
            String upHospitalCode = object.getString("upHospitalCode");                 // 上级医院CODE
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String repCode = object.getString("repCode");                               // REP岗位代码
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                   // postCode
            String region = object.getString("region");                   // region
            //20230529 START
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String upCustomerCode = object.getString("upCustomerCode");                   // 上级客户代码
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(sameTimeRetailCode) || StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(repCode)
                    || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            String lvl2Code = "";
//            if (lvlList.size() > 0) {
//                lvl2Code = lvlList.get(0).getLvl2Code();
//            } else {
//                //架构错误
//            }

            String lvl2Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_HOSPITAL);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_HOSPITAL);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**校验 架构城市关系*/
            int countFromStructureCity = customerPostMapper.queryCountFromStructureCity(nowYM, repCode, city);
            if (countFromStructureCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "架构城市关系错误", "架构城市关系不正确！");
            }


            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryHospitalDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, applyCode);
            int count3 = customerPostMapper.queryHospitalDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, applyCode);
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //获取repName,repCwid
            Map<String, String> repMap = customerPostMapper.getDataNameByDataCode(nowYM, repCode);
            String repName = null;
            String repCwid = null;
            if (!StringUtils.isEmpty(repMap)) {
                repName = repMap.get("userName");
                repCwid = repMap.get("cwid");
            } else {
                //架构错误
            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterHospitalAddDsm info = cuspostQuarterHospitalAddDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterHospitalAddDsm>()
                                .eq("applyCode", applyCode)
                );

                if (StringUtils.isEmpty(info)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    //地区经理只能改自己的，看不见大区助理的内容，这块查不到就是错了
                }
                /**更新*/
                UpdateWrapper<CuspostQuarterHospitalAddDsm> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("customerName", customerName);
                updateWrapper.set("province", province);
                updateWrapper.set("city", city);
                updateWrapper.set("address", address);
                updateWrapper.set("drugstoreProperty1Code", drugstoreProperty1Code);
                updateWrapper.set("sameTimeRetailCode", sameTimeRetailCode);
                updateWrapper.set("otherPropertyCode", otherPropertyCode);
                updateWrapper.set("upHospitalCode", upHospitalCode);
                updateWrapper.set("dsmCode", dsmCode);
                updateWrapper.set("dsmCwid", dsmCwid);
                updateWrapper.set("dsmName", dsmName);
                updateWrapper.set("repCode", repCode);
                updateWrapper.set("repCwid", repCwid);
                updateWrapper.set("repName", repName);
                updateWrapper.set("channelRemark", channelRemark);
                //20230529 START
                updateWrapper.set("territoryProducts", territoryProducts);
                updateWrapper.set("upCustomerCode", upCustomerCode);
                //20230529 END
                updateWrapper.set("updateTime", new Date());
                updateWrapper.set("updateUser", loginUser.getUserCode());
                updateWrapper.eq("applyCode", applyCode);
                int insertCount = cuspostQuarterHospitalAddDsmMapper.update(info, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterHospitalAddAssistant infoAssi = cuspostQuarterHospitalAddAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterHospitalAddAssistant>()
                                .eq("applyCode", applyCode)
                );
                if (StringUtils.isEmpty(infoAssi)) {
                    //如果不是大区助理的内容就去找地区经理
                    //获取既存数据
                    CuspostQuarterHospitalAddDsm infoDsm = cuspostQuarterHospitalAddDsmMapper.selectOne(
                            new QueryWrapper<CuspostQuarterHospitalAddDsm>()
                                    .eq("applyCode", applyCode)
                    );
                    if (StringUtils.isEmpty(infoDsm)) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    } else {
                        CuspostQuarterHospitalAddAssistant infoAssiInsert = new CuspostQuarterHospitalAddAssistant();
                        infoAssiInsert.setApplyCode(infoDsm.getApplyCode());
                        infoAssiInsert.setManageYear(BigDecimal.valueOf(manageYear));
                        infoAssiInsert.setManageQuarter(manageQuarter);
                        infoAssiInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                        infoAssiInsert.setCustomerTypeName("零售终端");//客户类型
                        infoAssiInsert.setRegion(infoDsm.getLvl2Code());//大区
                        infoAssiInsert.setCustomerName(customerName);
                        infoAssiInsert.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                        infoAssiInsert.setProvince(province);
                        infoAssiInsert.setCity(city);
                        infoAssiInsert.setAddress(address);
                        infoAssiInsert.setDrugstoreProperty1Code(drugstoreProperty1Code);
                        infoAssiInsert.setSameTimeRetailCode(sameTimeRetailCode);
                        infoAssiInsert.setOtherPropertyCode(otherPropertyCode);
                        infoAssiInsert.setUpHospitalCode(upHospitalCode);
                        infoAssiInsert.setDsmCode(dsmCode);
                        infoAssiInsert.setDsmCwid(dsmCwid);
                        infoAssiInsert.setDsmName(dsmName);
                        infoAssiInsert.setRepCode(repCode);
                        infoAssiInsert.setRepCwid(repCwid);
                        infoAssiInsert.setRepName(repName);
                        infoAssiInsert.setChannelRemark(channelRemark);
                        infoAssiInsert.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                        infoAssiInsert.setLvl2Code(infoDsm.getLvl2Code());
//                        infoAssiInsert.setLvl3Code(infoDsm.getLvl3Code());
                        infoAssiInsert.setLvl3Code(null);
                        infoAssiInsert.setLvl4Code(infoDsm.getLvl4Code());
                        //20230529 START
                        infoAssiInsert.setTerritoryProducts(territoryProducts);
                        infoAssiInsert.setUpCustomerCode(upCustomerCode);
                        //20230529 END

                        int insertCount = cuspostQuarterHospitalAddAssistantMapper.insert(infoAssiInsert);
                        if (insertCount <= 0) {
                            return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                        }
                    }
                } else {
                    /**更新*/
                    UpdateWrapper<CuspostQuarterHospitalAddAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("province", province);
                    updateWrapper.set("city", city);
                    updateWrapper.set("address", address);
                    updateWrapper.set("drugstoreProperty1Code", drugstoreProperty1Code);
                    updateWrapper.set("sameTimeRetailCode", sameTimeRetailCode);
                    updateWrapper.set("otherPropertyCode", otherPropertyCode);
                    updateWrapper.set("upHospitalCode", upHospitalCode);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("repCode", repCode);
                    updateWrapper.set("repCwid", repCwid);
                    updateWrapper.set("repName", repName);
                    updateWrapper.set("channelRemark", channelRemark);
                    //20230529 START
                    updateWrapper.set("territoryProducts", territoryProducts);
                    updateWrapper.set("upCustomerCode", upCustomerCode);
                    //20230529 END
                    updateWrapper.set("updateTime", new Date());
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.eq("applyCode", applyCode);
                    int insertCount = cuspostQuarterHospitalAddAssistantMapper.update(infoAssi, updateWrapper);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                    }
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 删除季度医院申请数据
     */
    @ApiOperation(value = "删除季度医院申请数据", notes = "删除季度医院申请数据")
    @RequestMapping(value = "/deleteHospitalApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper deleteHospitalApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String autoKey = object.getString("autoKey");                               // autoKey
            String applyCode = object.getString("applyCode");                               // 申请编码
            String postCode = object.getString("postCode");                               // postCode

            // 必须检查
            if (StringUtils.isEmpty(autoKey) || StringUtils.isEmpty(applyCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterHospitalAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterHospitalAddDsmMapper.delete(updateWrapper1);

            UpdateWrapper<CuspostQuarterHospitalAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterHospitalAddAssistantMapper.delete(updateWrapper2);
            if (insertCount1 <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据删除失败！");
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 上传季度医院申请数据
     */
    @ApiOperation(value = "上传季度医院申请数据", notes = "上传季度医院申请数据")
    @RequestMapping("/batchAddHospitalApplyQuarterInfo")
    @Transactional
    public Wrapper batchAddHospitalApplyQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_HOSPITAL_ADD);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_hospital_add_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_hospital_add_assistant";
                }
                String errorFileName = hospitalApplyQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String hospitalApplyQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 地区大区表查重（临时表已经有数据，check客户名称是否在地区经理表，大区助理表，核心表中存在，流向年月+客户名称）
            String cusNames1 = customerPostMapper.queryHospitalDsmCusDuplicateFromSelfByTon(
                    tableEnNameTem, manageYear, manageQuarter, fileId);
            if (cusNames1 != null) {
                String messageContent = " 客户名称 【" + cusNames1 + "】在本季新增中已存在，请确认！";
                errorMessageList.add(messageContent);
            }
            //check 核心表
            String cusNames2 = customerPostMapper.queryHospitalDsmCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE1);
//            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE1);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }

            /**校验 架构城市关系*/
            String relation2 = customerPostMapper.updateCheckFromStructureCity(
                    tableEnNameTem, nowYM, manageMonth, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE2, null);
            if (relation2 != null) {
                String messageContent = " 客户 【" + relation2 + "】的架构城市关系不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                /**获取上传数*/
                int count = customerPostMapper.queryCountForUpload(UserConstant.UPLOAD_TABLE_PREFIX + tableEnName, fileId);

                /**创建applyCode申请编码*/
//                int applyCodeInt = this.getApplyCodeBatch(manageYear, manageQuarter, count);

                //更新 终端类型，大区，postCode，lvl2Code，lvl3Code，lvl4Code
                customerPostMapper.uploadHospitalApplyQuarterInfoOther(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth, nowYM
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);

                // 更新上传数据 页面可以修改 影响applyCode批量创建
                // 插入上传数据
//                customerPostMapper.uploadHospitalApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode, applyCodeInt);
                customerPostMapper.uploadHospitalApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode);// 20230424 新增申请编码
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 提交季度医院申请数据
     */
    @ApiOperation(value = "提交季度医院申请数据", notes = "提交季度医院申请数据")
    @RequestMapping(value = "/submitHospitalApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper submitHospitalApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String customerTypeCode = object.getString("customerTypeCode"); // 1医院,2零售,3商务,4连锁
            String applyJudge = object.getString("applyJudge"); // 大于等于小于
            String postCode = object.getString("postCode"); // 岗位编码，1：地区经理，2：大区助理
            String assistantRemark = object.getString("assistantRemark"); // 大区助理备注
            String region = object.getString("region");                   // region

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCodeDsmAssistant(nowYM, postCode, loginUser.getUserCode());
//            String lvl2Code = "";
//            String lvl3Code = "";
//            String lvl4Code = "";
//            String assistantName = "";
//            String lvl3Name = "";
//            if (lvlList.size() > 0) {
//                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//                assistantName = lvlList.get(0).getAssistantName();
//                lvl3Name = lvlList.get(0).getLvl3Name();
//            } else {
//                //架构错误
//            }
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }
            /**地区经理提交 cuspost_quarter_apply_state_info*/
            String approver = "";//当前审批人
            String buttonEffect = "";//按钮是否有效

            if (UserConstant.POST_CODE1.equals(postCode) && UserConstant.CUSTOMER_TYPE_HOSPITAL.equals(customerTypeCode)) {
                //地区经理查询自己的数据
                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    List<CuspostQuarterHospitalAddDsm> existList = cuspostQuarterHospitalAddDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterHospitalAddDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    List<CuspostQuarterHospitalChangeDsm> existList = cuspostQuarterHospitalChangeDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterHospitalChangeDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }

                //按钮是否有效
                buttonEffect = "hospitalButtonEffect";
                //获取审批人
//                approver = assistantName;
                approver = "大区助理";

                //更新申请编码状态 cuspost_quarter_apply_state_info
                CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("hospitalApplyStateCode", UserConstant.APPLY_STATE_CODE_2);
                updateWrapper.eq("typeCode", typeCode);
                updateWrapper.eq("manageYear", manageYear);
                updateWrapper.eq("manageQuarter", manageQuarter);
                updateWrapper.eq("lvl4Code", lvl4Code);
                int insertCount = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                }

                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    //更新申请编码状态 cuspost_quarter_hospital_add_dsm
                    CuspostQuarterHospitalAddDsm cuspostHospitalApplyQuarterInfo = new CuspostQuarterHospitalAddDsm();
                    UpdateWrapper<CuspostQuarterHospitalAddDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterHospitalAddDsmMapper.update(cuspostHospitalApplyQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    //更新申请编码状态 cuspost_quarter_hospital_change_dsm
                    CuspostQuarterHospitalChangeDsm cuspostHospitalChangeDeletionQuarterInfo = new CuspostQuarterHospitalChangeDsm();
                    UpdateWrapper<CuspostQuarterHospitalChangeDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterHospitalChangeDsmMapper.update(cuspostHospitalChangeDeletionQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
            }

            /**大区助理提交
             * cuspost_quarter_hospital_add_dsm
             * cuspost_quarter_hospital_add_assistant
             * cuspost_quarter_hospital_change_dsm
             * cuspost_quarter_hospital_change_assistant
             * cuspost_quarter_apply_state_info
             * cuspost_quarter_apply_state_region_info
             * */
            if (UserConstant.POST_CODE2.equals(postCode) && UserConstant.CUSTOMER_TYPE_HOSPITAL.equals(customerTypeCode)) {
                //大区助理查询地区和大区的数据
                List<CuspostQuarterHospitalAddDsm> existList1 = cuspostQuarterHospitalAddDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterHospitalAddDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterHospitalAddAssistant> existList2 = cuspostQuarterHospitalAddAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterHospitalAddAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterHospitalChangeDsm> existList3 = cuspostQuarterHospitalChangeDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterHospitalChangeDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterHospitalChangeAssistant> existList4 = cuspostQuarterHospitalChangeAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterHospitalChangeAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                if ((StringUtils.isEmpty(existList1) || existList1.size() < 1)
                        && (StringUtils.isEmpty(existList2) || existList2.size() < 1)
                        && (StringUtils.isEmpty(existList3) || existList3.size() < 1)
                        && (StringUtils.isEmpty(existList4) || existList4.size() < 1)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                }

                //按钮是否有效
                buttonEffect = "hospitalButtonEffect";
                //查询审批人：approver
//                approver = lvl3Name;
                approver = "大区总监";

                /**更新申请编码状态 cuspost_quarter_hospital_add_dsm*/
                CuspostQuarterHospitalAddDsm info1 = new CuspostQuarterHospitalAddDsm();
                UpdateWrapper<CuspostQuarterHospitalAddDsm> updateWrapper1 = new UpdateWrapper<>();
                updateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper1.set("approver", approver);
                updateWrapper1.set("verifyRemark", assistantRemark);
                updateWrapper1.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper1.eq("manageYear", manageYear);
                updateWrapper1.eq("manageQuarter", manageQuarter);
                updateWrapper1.eq("lvl2Code", lvl2Code);
                int insertCount1 = cuspostQuarterHospitalAddDsmMapper.update(info1, updateWrapper1);

                /**更新申请编码状态 cuspost_quarter_hospital_add_assistant*/
                CuspostQuarterHospitalAddAssistant info2 = new CuspostQuarterHospitalAddAssistant();
                UpdateWrapper<CuspostQuarterHospitalAddAssistant> updateWrapper2 = new UpdateWrapper<>();
                updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper2.set("approver", approver);
                updateWrapper2.set("verifyRemark", assistantRemark);
                updateWrapper2.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper2.eq("manageYear", manageYear);
                updateWrapper2.eq("manageQuarter", manageQuarter);
                updateWrapper2.eq("lvl2Code", lvl2Code);
                int insertCount2 = cuspostQuarterHospitalAddAssistantMapper.update(info2, updateWrapper2);

                /**更新申请编码状态 cuspost_quarter_hospital_change_dsm*/
                CuspostQuarterHospitalChangeDsm info3 = new CuspostQuarterHospitalChangeDsm();
                UpdateWrapper<CuspostQuarterHospitalChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper3.set("approver", approver);
                updateWrapper3.set("verifyRemark", assistantRemark);
                updateWrapper3.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl2Code", lvl2Code);
                int insertCount3 = cuspostQuarterHospitalChangeDsmMapper.update(info3, updateWrapper3);

                /**更新申请编码状态 cuspost_quarter_hospital_change_assistant*/
                CuspostQuarterHospitalChangeAssistant info4 = new CuspostQuarterHospitalChangeAssistant();
                UpdateWrapper<CuspostQuarterHospitalChangeAssistant> updateWrapper4 = new UpdateWrapper<>();
                updateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper4.set("approver", approver);
                updateWrapper4.set("verifyRemark", assistantRemark);
                updateWrapper4.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper4.eq("manageYear", manageYear);
                updateWrapper4.eq("manageQuarter", manageQuarter);
                updateWrapper4.eq("lvl2Code", lvl2Code);
                int insertCount4 = cuspostQuarterHospitalChangeAssistantMapper.update(info4, updateWrapper4);

                /**更新申请编码状态 cuspost_quarter_apply_state_info*/
                CuspostQuarterApplyStateInfo info5 = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper5 = new UpdateWrapper<>();
                updateWrapper5.set("hospitalApplyStateCode", UserConstant.APPLY_STATE_CODE_4);
                //updateWrapper5.eq("typeCode", typeCode); 大区助理提交时，新增和变更删除都一起提交
                updateWrapper5.eq("manageYear", manageYear);
                updateWrapper5.eq("manageQuarter", manageQuarter);
                updateWrapper5.eq("lvl2Code", lvl2Code);
                int insertCount5 = cuspostQuarterApplyStateInfoMapper.update(info5, updateWrapper5);

                //20230613 没有数据的状态变为已完成 START
                customerPostMapper.updateQuarterApplyStateHospitalAddAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateHospitalAddDsmNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateHospitalChangeAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateHospitalChangeDsmNoData(manageYear, manageQuarter, lvl2Code);
                //20230613 没有数据的状态变为已完成 END

                /**更新申请编码状态 cuspost_quarter_apply_state_region_info*/
                CuspostQuarterApplyStateRegionInfo insertModel = new CuspostQuarterApplyStateRegionInfo();
                insertModel.setManageYear(BigDecimal.valueOf(manageYear));
                insertModel.setManageQuarter(manageQuarter);
                insertModel.setCustomerTypeCode(customerTypeCode);
                insertModel.setRegion(lvl2Code);
                insertModel.setApplyStateCode(UserConstant.APPLY_STATE_CODE_4);
                insertModel.setPostCode(UserConstant.POST_CODE3);
                insertModel.setIsOver("0");
                if ("大于".equals(applyJudge)) {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A004");
                    } else {
                        insertModel.setApprovalProcessCode("A002");
                    }

                } else {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A003");
                    } else {
                        insertModel.setApprovalProcessCode("A001");
                    }
                }
                //20230613 如果没有数据不进入到总监审批 START
                if ((!StringUtils.isEmpty(existList1) && existList1.size() > 0)
                        || (!StringUtils.isEmpty(existList2) && existList2.size() > 0)) {
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE1);
                    int insertCountInsert1 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                if ((!StringUtils.isEmpty(existList3) && existList3.size() > 0)
                        || (!StringUtils.isEmpty(existList4) && existList4.size() > 0)) {
                    //变更删除的场合再创建一遍
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE2);
                    int insertCountInsert2 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                //20230613 如果没有数据不进入到总监审批 END

            }
            resultMap.put(buttonEffect, "0"); //按钮不可用

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 查询季度零售终端申请数据
     */
    @ApiOperation(value = "查询季度零售终端申请数据", notes = "查询季度零售终端申请数据")
    @RequestMapping(value = "/queryRetailApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryRetailApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String postCode = object.getString("postCode"); // postCode
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryRetailApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, drugstoreProperty1Code
                    , region
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载季度零售终端申请数据
     */
    @ApiOperation(value = "下载季度零售终端申请数据", notes = "下载季度零售终端申请数据")
    @RequestMapping(value = "/exprotRetailApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotRetailApplyQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryRetailApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, drugstoreProperty1Code
                    , region
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_RETAIL_ADD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度零售终端申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * @MethodName 下载季度零售终端申请数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度零售终端申请数据 D&A下载按照上传模板顺序", notes = "下载季度零售终端申请数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotRetailApplyQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotRetailApplyQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryRetailApplyQuarterInfo(page
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, drugstoreProperty1Code
                    , region
                    , orderName
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_RETAIL_APPLY_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度零售终端申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * 新增季度零售终端申请数据
     */
    @ApiOperation(value = "新增季度零售终端申请数据", notes = "新增季度零售终端申请数据")
    @RequestMapping(value = "/addRetailApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper addRetailApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                             // 省份
            String city = object.getString("city");                                     // 城市
            String address = object.getString("address");                               // 地址
            String propertyCode = object.getString("propertyCode");                     // 属性
            String upCode = object.getString("upCode");                                 // 上级代码
            String upName = object.getString("upName");                                 // 上级名称
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                             // postCode
            String region = object.getString("region");                   // region
            //20230529 START
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());

            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_RETAIL);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_RETAIL);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }

            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryRetailDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, "");
            int count3 = customerPostMapper.queryRetailDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, "");
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }


            /**创建applyCode申请编码*/
//            String applyCodeStr = this.getApplyCode(manageYear, manageQuarter);
//            if (StringUtils.isEmpty(applyCodeStr)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "错误", "申请编码没有数据！");
//            }
            String applyCodeStr = commonUtils.createUUID().replaceAll("-", "");// 20230424 新增申请编码

            /**插入数据表*/
            if (UserConstant.POST_CODE1.equals(postCode)) {
                CuspostQuarterRetailAddDsm info = new CuspostQuarterRetailAddDsm();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("零售终端");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setPropertyCode(propertyCode);
                info.setUpCode(upCode);
                info.setUpName(upName);
                info.setDrugstoreProperty1Code(drugstoreProperty1Code);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTerritoryProducts(territoryProducts);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterRetailAddDsmMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }

            if (UserConstant.POST_CODE2.equals(postCode)) {
                CuspostQuarterRetailAddAssistant info = new CuspostQuarterRetailAddAssistant();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("零售终端");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setPropertyCode(propertyCode);
                info.setUpCode(upCode);
                info.setUpName(upName);
                info.setDrugstoreProperty1Code(drugstoreProperty1Code);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTerritoryProducts(territoryProducts);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterRetailAddAssistantMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 修改季度零售终端申请数据
     */
    @ApiOperation(value = "修改季度零售终端申请数据", notes = "更新季度零售终端申请数据")
    @RequestMapping(value = "/updateRetailApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper updateRetailApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String applyCode = object.getString("applyCode");                  // 申请编码
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                     // 省份
            String city = object.getString("city");                             // 城市
            String address = object.getString("address");                               // 地址
            String propertyCode = object.getString("propertyCode");                     // 属性
            String upCode = object.getString("upCode");                                 // 上级代码
            String upName = object.getString("upName");                                 // 上级名称
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 药店属性1
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                   // postCode
            String region = object.getString("region");                   // region
            //20230529 START
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(applyCode) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());

            String lvl2Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_RETAIL);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_RETAIL);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryRetailDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, applyCode);
            int count3 = customerPostMapper.queryRetailDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, applyCode);
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterRetailAddDsm info = cuspostQuarterRetailAddDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterRetailAddDsm>()
                                .eq("applyCode", applyCode)
                );

                if (StringUtils.isEmpty(info)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    //地区经理只能改自己的，看不见大区助理的内容，这块查不到就是错了
                }
                /**更新*/
                UpdateWrapper<CuspostQuarterRetailAddDsm> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("customerName", customerName);
                updateWrapper.set("province", province);
                updateWrapper.set("city", city);
                updateWrapper.set("address", address);
                updateWrapper.set("propertyCode", propertyCode);
                updateWrapper.set("upCode", upCode);
                updateWrapper.set("upName", upName);
                updateWrapper.set("drugstoreProperty1Code", drugstoreProperty1Code);
                updateWrapper.set("channelRemark", channelRemark);
                //20230529 START
                updateWrapper.set("territoryProducts", territoryProducts);
                //20230529 END
                updateWrapper.set("updateTime", new Date());
                updateWrapper.set("updateUser", loginUser.getUserCode());
                updateWrapper.eq("applyCode", applyCode);
                int insertCount = cuspostQuarterRetailAddDsmMapper.update(info, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterRetailAddAssistant infoAssi = cuspostQuarterRetailAddAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterRetailAddAssistant>()
                                .eq("applyCode", applyCode)
                );
                if (StringUtils.isEmpty(infoAssi)) {
                    //获取既存数据
                    CuspostQuarterRetailAddDsm infoDsm = cuspostQuarterRetailAddDsmMapper.selectOne(
                            new QueryWrapper<CuspostQuarterRetailAddDsm>()
                                    .eq("applyCode", applyCode)
                    );
                    if (StringUtils.isEmpty(infoDsm)) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    } else {
                        CuspostQuarterRetailAddAssistant infoAssiInsert = new CuspostQuarterRetailAddAssistant();
                        infoAssiInsert.setApplyCode(infoDsm.getApplyCode());
                        infoAssiInsert.setManageYear(BigDecimal.valueOf(manageYear));
                        infoAssiInsert.setManageQuarter(manageQuarter);
                        infoAssiInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                        infoAssiInsert.setCustomerTypeName("零售终端");//客户类型
                        infoAssiInsert.setRegion(infoDsm.getLvl2Code());//大区
                        infoAssiInsert.setCustomerName(customerName);
                        infoAssiInsert.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                        infoAssiInsert.setProvince(province);
                        infoAssiInsert.setCity(city);
                        infoAssiInsert.setAddress(address);
                        infoAssiInsert.setPropertyCode(propertyCode);
                        infoAssiInsert.setUpCode(upCode);
                        infoAssiInsert.setUpName(upName);
                        infoAssiInsert.setDrugstoreProperty1Code(drugstoreProperty1Code);
                        infoAssiInsert.setChannelRemark(channelRemark);
                        infoAssiInsert.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                        infoAssiInsert.setLvl2Code(infoDsm.getLvl2Code());
//                        infoAssiInsert.setLvl3Code(infoDsm.getLvl3Code());
                        infoAssiInsert.setLvl3Code(null);
                        infoAssiInsert.setLvl4Code(infoDsm.getLvl4Code());
                        //20230529 START
                        infoAssiInsert.setTerritoryProducts(territoryProducts);
                        //20230529 END

                        int insertCount = cuspostQuarterRetailAddAssistantMapper.insert(infoAssiInsert);
                        if (insertCount <= 0) {
                            return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                        }
                    }
                } else {
                    /**更新*/
                    UpdateWrapper<CuspostQuarterRetailAddAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("province", province);
                    updateWrapper.set("city", city);
                    updateWrapper.set("address", address);
                    updateWrapper.set("propertyCode", propertyCode);
                    updateWrapper.set("upCode", upCode);
                    updateWrapper.set("upName", upName);
                    updateWrapper.set("drugstoreProperty1Code", drugstoreProperty1Code);
                    updateWrapper.set("channelRemark", channelRemark);
                    //20230529 START
                    updateWrapper.set("territoryProducts", territoryProducts);
                    //20230529 END
                    updateWrapper.set("updateTime", new Date());
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.eq("applyCode", applyCode);
                    int insertCount = cuspostQuarterRetailAddAssistantMapper.update(infoAssi, updateWrapper);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                    }
                }
            }


        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 删除季度零售终端申请数据
     */
    @ApiOperation(value = "删除季度零售终端申请数据", notes = "删除季度零售终端申请数据")
    @RequestMapping(value = "/deleteRetailApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper deleteRetailApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String autoKey = object.getString("autoKey");                               // autoKey
            String applyCode = object.getString("applyCode");                               // 申请编码
            String postCode = object.getString("postCode");                               // postCode

            // 必须检查
            if (StringUtils.isEmpty(autoKey) || StringUtils.isEmpty(applyCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterRetailAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterRetailAddDsmMapper.delete(updateWrapper1);

            UpdateWrapper<CuspostQuarterRetailAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterRetailAddAssistantMapper.delete(updateWrapper2);
            if (insertCount1 <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据删除失败！");
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 上传季度零售终端申请数据
     */
    @ApiOperation(value = "上传季度零售终端申请数据", notes = "上传季度零售终端申请数据")
    @RequestMapping("/batchAddRetailApplyQuarterInfo")
    @Transactional
    public Wrapper batchAddRetailApplyQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");                   // region

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_RETAIL_ADD);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_retail_add_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_retail_add_assistant";
                }
                String errorFileName = retailApplyQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String retailApplyQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 地区大区表查重（临时表已经有数据，check客户名称是否在地区经理表，大区助理表，核心表中存在，流向年月+客户名称）
            String cusNames1 = customerPostMapper.queryRetailDsmCusDuplicateFromSelfByTon(
                    tableEnNameTem, manageYear, manageQuarter, fileId);
            if (cusNames1 != null) {
                String messageContent = " 客户名称 【" + cusNames1 + "】在本季新增中已存在，请确认！";
                errorMessageList.add(messageContent);
            }
            //check 核心表
            String cusNames2 = customerPostMapper.queryRetailDsmCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_RETAIL, fileId, UserConstant.APPLY_TYPE_CODE1);
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_RETAIL, fileId, UserConstant.APPLY_TYPE_CODE1);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                /**获取上传数*/
                int count = customerPostMapper.queryCountForUpload(UserConstant.UPLOAD_TABLE_PREFIX + tableEnName, fileId);

                /**创建applyCode申请编码*/
//                int applyCodeInt = this.getApplyCodeBatch(manageYear, manageQuarter, count);

                //更新 终端类型，大区，postCode，lvl2Code，lvl3Code，lvl4Code
                customerPostMapper.uploadRetailApplyQuarterInfoOther(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);

                // 更新上传数据 页面可以修改 影响applyCode批量创建
                // 插入上传数据
//                customerPostMapper.uploadRetailApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode, applyCodeInt);
                customerPostMapper.uploadRetailApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode);// 20230424 新增申请编码
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 大区助理提交，计算客户总数量等
     */
    @ApiOperation(value = "计算客户总数量等", notes = "计算客户总数量等")
    @RequestMapping(value = "/queryTotalForSubmitByAssistant", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryTotalForSubmitByAssistant(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerTypeCode = object.getString("customerTypeCode"); // 1医院,2零售,3商务,4连锁
            String region = object.getString("region"); // 商务大区
            String lvl2Code = region;

            // 必须检查
            if (UserConstant.CUSTOMER_TYPE_DISTRIBUTOR.equals(customerTypeCode) && StringUtils.isEmpty(region)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "商务大区不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            double applyAddCount = 0;
            double applyDeletionCount = 0;
            double applyTotalCount = 0;
            String applyJudge = null;
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, "2", loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryAssistantLevelCode(nowYM, loginUser.getUserCode());
//
//            String lvl2Code = "";
//            if (lvlList.size() > 0) {
//                lvl2Code = lvlList.get(0).getLvl2Code();
//            } else {
//                //架构错误
//            }
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            /**医院*/
            if (UserConstant.CUSTOMER_TYPE_HOSPITAL.equals(customerTypeCode)) {
                //累计申请新增X家
                applyAddCount = customerPostMapper.queryHospitalCountForAdd(manageYear, manageQuarter, manageMonth, lvl2Code);
                //累计申请删除X家
//                applyDeletionCount = customerPostMapper.queryHospitalCountForDeletion(manageYear, manageQuarter, manageMonth, lvl2Code);
                applyDeletionCount = customerPostMapper.queryHospitalCountForDeletion(manageMonth, lvl2Code);
//                applyDeletionCount = 0;
                //当季该客户总数量
                applyTotalCount = customerPostMapper.queryHospitalCountForTotal(manageMonth, lvl2Code);
            }

            /**零售终端*/
            if (UserConstant.CUSTOMER_TYPE_RETAIL.equals(customerTypeCode)) {
                //累计申请新增X家
                applyAddCount = customerPostMapper.queryRetailCountForAdd(manageYear, manageQuarter, manageMonth, lvl2Code);
                //累计申请删除X家
//                applyDeletionCount = customerPostMapper.queryRetailCountForDeletion(manageYear, manageQuarter, manageMonth, lvl2Code);
                applyDeletionCount = customerPostMapper.queryRetailCountForDeletion(manageMonth, lvl2Code);
                //当季该客户总数量
                applyTotalCount = customerPostMapper.queryRetailCountForTotal(manageMonth, lvl2Code);
            }

            /**商务打单商*/
            if (UserConstant.CUSTOMER_TYPE_DISTRIBUTOR.equals(customerTypeCode)) {
                //累计申请新增X家
                applyAddCount = customerPostMapper.queryDistributorCountForAdd(manageYear, manageQuarter, manageMonth, lvl2Code);
                //累计申请删除X家
//                applyDeletionCount = customerPostMapper.queryDistributorCountForDeletion(manageYear, manageQuarter, manageMonth, lvl2Code);
                applyDeletionCount = customerPostMapper.queryDistributorCountForDeletion(manageMonth, lvl2Code);
//                applyDeletionCount = 0;
                //当季该客户总数量
                applyTotalCount = customerPostMapper.queryDistributorCountForTotal(manageMonth, lvl2Code);
            }

            /**连锁总部*/
            if (UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ.equals(customerTypeCode)) {
                //累计申请新增X家
                applyAddCount = customerPostMapper.queryChainstoreHqCountForAdd(manageYear, manageQuarter, manageMonth, lvl2Code);
                //累计申请删除X家
//                applyDeletionCount = customerPostMapper.queryChainstoreHqCountForDeletion(manageYear, manageQuarter, manageMonth, lvl2Code);
                applyDeletionCount = customerPostMapper.queryChainstoreHqCountForDeletion(manageMonth, lvl2Code);
//                applyDeletionCount = 0;
                //当季该客户总数量
                applyTotalCount = customerPostMapper.queryChainstoreHqCountForTotal(manageMonth, lvl2Code);
            }
            if (applyTotalCount == 0.0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "计算错误", "基础数据不存在，请确认");
            }

            double percent = (applyAddCount / (applyTotalCount - applyDeletionCount));

            if (percent > 0.01) {
                applyJudge = "大于";
            }
            if (percent < 0.01) {
                applyJudge = "小于";
            }
            if (percent == 0.01) {
                applyJudge = "等于";
            }
            // 检索处理
//            List<Map<String, Object>> list = new ArrayList<>();
            Map<String, Object> map = new HashMap<>();
            map.put("applyAddCount", applyAddCount);
            map.put("applyDeletionCount", applyDeletionCount);
            map.put("applyJudge", applyJudge);


            resultMap.put("list", map);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 提交季度零售终端申请数据
     */
    @ApiOperation(value = "提交季度零售终端申请数据", notes = "提交季度零售终端申请数据")
    @RequestMapping(value = "/submitRetailApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper submitRetailApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String customerTypeCode = object.getString("customerTypeCode"); // 1医院,2零售,3商务,4连锁
            String applyJudge = object.getString("applyJudge"); // 大于等于小于
            String postCode = object.getString("postCode"); // 岗位编码，1：地区经理，2：大区助理
            String assistantRemark = object.getString("assistantRemark"); // 大区助理备注
            String region = object.getString("region");                   // region

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCodeDsmAssistant(nowYM, postCode, loginUser.getUserCode());
//            String lvl2Code = "";
//            String lvl3Code = "";
//            String lvl4Code = "";
//            String assistantName = "";
//            String lvl3Name = "";
//            if (lvlList.size() > 0) {
//                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//                assistantName = lvlList.get(0).getAssistantName();
//                lvl3Name = lvlList.get(0).getLvl3Name();
//            } else {
//                //架构错误
//            }
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**地区经理提交 cuspost_quarter_apply_state_info*/
            String approver = "";//当前审批人
            String buttonEffect = "";//按钮是否有效

            if (UserConstant.POST_CODE1.equals(postCode) && UserConstant.CUSTOMER_TYPE_RETAIL.equals(customerTypeCode)) {
                //地区经理查询自己的数据
                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    List<CuspostQuarterRetailAddDsm> existList = cuspostQuarterRetailAddDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterRetailAddDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    List<CuspostQuarterRetailChangeDsm> existList = cuspostQuarterRetailChangeDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterRetailChangeDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }

                //按钮是否有效
                buttonEffect = "retailButtonEffect";
                //获取审批人
//                approver = assistantName;
                approver = "大区助理";

                //更新申请编码状态 cuspost_quarter_apply_state_info
                CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("retailApplyStateCode", UserConstant.APPLY_STATE_CODE_2);
                updateWrapper.eq("typeCode", typeCode);
                updateWrapper.eq("manageYear", manageYear);
                updateWrapper.eq("manageQuarter", manageQuarter);
                updateWrapper.eq("lvl4Code", lvl4Code);
                int insertCount = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                }

                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    //更新申请编码状态 cuspost_quarter_retail_add_dsm
                    CuspostQuarterRetailAddDsm cuspostRetailApplyQuarterInfo = new CuspostQuarterRetailAddDsm();
                    UpdateWrapper<CuspostQuarterRetailAddDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterRetailAddDsmMapper.update(cuspostRetailApplyQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    //更新申请编码状态 cuspost_quarter_retail_change_dsm
                    CuspostQuarterRetailChangeDsm cuspostRetailChangeDeletionQuarterInfo = new CuspostQuarterRetailChangeDsm();
                    UpdateWrapper<CuspostQuarterRetailChangeDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterRetailChangeDsmMapper.update(cuspostRetailChangeDeletionQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
            }

            /**大区助理提交
             * cuspost_quarter_retail_add_dsm
             * cuspost_quarter_retail_add_assistant
             * cuspost_quarter_retail_change_dsm
             * cuspost_quarter_retail_change_assistant
             * cuspost_quarter_apply_state_info
             * cuspost_quarter_apply_state_region_info
             * */
            if (UserConstant.POST_CODE2.equals(postCode) && UserConstant.CUSTOMER_TYPE_RETAIL.equals(customerTypeCode)) {
                //大区助理查询地区和大区的数据
                List<CuspostQuarterRetailAddDsm> existList1 = cuspostQuarterRetailAddDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterRetailAddDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterRetailAddAssistant> existList2 = cuspostQuarterRetailAddAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterRetailAddAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterRetailChangeDsm> existList3 = cuspostQuarterRetailChangeDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterRetailChangeDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterRetailChangeAssistant> existList4 = cuspostQuarterRetailChangeAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterRetailChangeAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                if ((StringUtils.isEmpty(existList1) || existList1.size() < 1)
                        && (StringUtils.isEmpty(existList2) || existList2.size() < 1)
                        && (StringUtils.isEmpty(existList3) || existList3.size() < 1)
                        && (StringUtils.isEmpty(existList4) || existList4.size() < 1)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                }

                //按钮是否有效
                buttonEffect = "retailButtonEffect";
                //查询审批人：approver
//                approver = lvl3Name;
                approver = "大区总监";

                /**更新申请编码状态 cuspost_quarter_retail_add_dsm*/
                CuspostQuarterRetailAddDsm info1 = new CuspostQuarterRetailAddDsm();
                UpdateWrapper<CuspostQuarterRetailAddDsm> updateWrapper1 = new UpdateWrapper<>();
                updateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper1.set("approver", approver);
                updateWrapper1.set("verifyRemark", assistantRemark);
                updateWrapper1.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper1.eq("manageYear", manageYear);
                updateWrapper1.eq("manageQuarter", manageQuarter);
                updateWrapper1.eq("lvl2Code", lvl2Code);
                int insertCount1 = cuspostQuarterRetailAddDsmMapper.update(info1, updateWrapper1);

                /**更新申请编码状态 cuspost_quarter_retail_add_assistant*/
                CuspostQuarterRetailAddAssistant info2 = new CuspostQuarterRetailAddAssistant();
                UpdateWrapper<CuspostQuarterRetailAddAssistant> updateWrapper2 = new UpdateWrapper<>();
                updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper2.set("approver", approver);
                updateWrapper2.set("verifyRemark", assistantRemark);
                updateWrapper2.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper2.eq("manageYear", manageYear);
                updateWrapper2.eq("manageQuarter", manageQuarter);
                updateWrapper2.eq("lvl2Code", lvl2Code);
                int insertCount2 = cuspostQuarterRetailAddAssistantMapper.update(info2, updateWrapper2);

                /**更新申请编码状态 cuspost_quarter_retail_change_dsm*/
                CuspostQuarterRetailChangeDsm info3 = new CuspostQuarterRetailChangeDsm();
                UpdateWrapper<CuspostQuarterRetailChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper3.set("approver", approver);
                updateWrapper3.set("verifyRemark", assistantRemark);
                updateWrapper3.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl2Code", lvl2Code);
                int insertCount3 = cuspostQuarterRetailChangeDsmMapper.update(info3, updateWrapper3);

                /**更新申请编码状态 cuspost_quarter_retail_change_assistant*/
                CuspostQuarterRetailChangeAssistant info4 = new CuspostQuarterRetailChangeAssistant();
                UpdateWrapper<CuspostQuarterRetailChangeAssistant> updateWrapper4 = new UpdateWrapper<>();
                updateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper4.set("approver", approver);
                updateWrapper4.set("verifyRemark", assistantRemark);
                updateWrapper4.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper4.eq("manageYear", manageYear);
                updateWrapper4.eq("manageQuarter", manageQuarter);
                updateWrapper4.eq("lvl2Code", lvl2Code);
                int insertCount4 = cuspostQuarterRetailChangeAssistantMapper.update(info4, updateWrapper4);

                /**更新申请编码状态 cuspost_quarter_apply_state_info*/
                CuspostQuarterApplyStateInfo info5 = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper5 = new UpdateWrapper<>();
                updateWrapper5.set("retailApplyStateCode", UserConstant.APPLY_STATE_CODE_4);
                //updateWrapper5.eq("typeCode", typeCode); 大区助理提交时，新增和变更删除都一起提交
                updateWrapper5.eq("manageYear", manageYear);
                updateWrapper5.eq("manageQuarter", manageQuarter);
                updateWrapper5.eq("lvl2Code", lvl2Code);
                int insertCount5 = cuspostQuarterApplyStateInfoMapper.update(info5, updateWrapper5);

                //20230613 没有数据的状态变为已完成 START
                customerPostMapper.updateQuarterApplyStateRetailAddAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateRetailAddDsmNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateRetailChangeAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateRetailChangeDsmNoData(manageYear, manageQuarter, lvl2Code);
                //20230613 没有数据的状态变为已完成 END


                /**更新申请编码状态 cuspost_quarter_apply_state_region_info*/
                CuspostQuarterApplyStateRegionInfo insertModel = new CuspostQuarterApplyStateRegionInfo();
                insertModel.setManageYear(BigDecimal.valueOf(manageYear));
                insertModel.setManageQuarter(manageQuarter);
                insertModel.setCustomerTypeCode(customerTypeCode);
                insertModel.setRegion(lvl2Code);
                insertModel.setApplyStateCode(UserConstant.APPLY_STATE_CODE_4);
                insertModel.setPostCode(UserConstant.POST_CODE3);
                insertModel.setIsOver("0");
                if ("大于".equals(applyJudge)) {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A004");
                    } else {
                        insertModel.setApprovalProcessCode("A002");
                    }

                } else {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A003");
                    } else {
                        insertModel.setApprovalProcessCode("A001");
                    }
                }
                //20230613 如果没有数据不进入到总监审批 START
                if ((!StringUtils.isEmpty(existList1) && existList1.size() > 0)
                        || (!StringUtils.isEmpty(existList2) && existList2.size() > 0)) {
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE1);
                    int insertCountInsert1 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                if ((!StringUtils.isEmpty(existList3) && existList3.size() > 0)
                        || (!StringUtils.isEmpty(existList4) && existList4.size() > 0)) {
                    //变更删除的场合再创建一遍
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE2);
                    int insertCountInsert2 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                //20230613 如果没有数据不进入到总监审批 END
            }
            resultMap.put(buttonEffect, "0"); //按钮不可用

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 退回大区修改（医院）
     */
    @ApiOperation(value = "退回大区修改（医院）", notes = "退回大区修改（医院）")
    @RequestMapping(value = "/sendBackHospitalQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper sendBackHospitalQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String dsmCode = object.getString("dsmCode"); // 申请人编码：dsmCode
            String sendBackInstruction = object.getString("sendBackInstruction"); // 退回说明：sendBackInstruction

            CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
            UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("hospitalApplyStateCode", UserConstant.APPLY_STATE_CODE_3);
            updateWrapper2.eq("typeCode", typeCode);
            updateWrapper2.eq("manageYear", manageYear);
            updateWrapper2.eq("manageQuarter", manageQuarter);
            updateWrapper2.eq("lvl4Code", dsmCode);
            int insertCount2 = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper2);

            /**更新审批意见*/
            if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {
                CuspostQuarterHospitalAddDsm cuspostHospitalApplyQuarterInfo = new CuspostQuarterHospitalAddDsm();
                UpdateWrapper<CuspostQuarterHospitalAddDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterHospitalAddDsmMapper.update(cuspostHospitalApplyQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {
                CuspostQuarterHospitalChangeDsm cuspostHospitalChangeDeletionQuarterInfo = new CuspostQuarterHospitalChangeDsm();
                UpdateWrapper<CuspostQuarterHospitalChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterHospitalChangeDsmMapper.update(cuspostHospitalChangeDeletionQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 退回大区修改（零售终端）
     */
    @ApiOperation(value = "退回大区修改（零售终端）", notes = "退回大区修改（零售终端）")
    @RequestMapping(value = "/sendBackRetailQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper sendBackRetailQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String dsmCode = object.getString("dsmCode"); // 申请人编码：dsmCode
            String sendBackInstruction = object.getString("sendBackInstruction"); // 退回说明：sendBackInstruction

            //更新申请编码状态 cuspost_quarter_apply_state_info
            CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
            UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("retailApplyStateCode", UserConstant.APPLY_STATE_CODE_3);
            updateWrapper2.eq("typeCode", typeCode);
            updateWrapper2.eq("manageYear", manageYear);
            updateWrapper2.eq("manageQuarter", manageQuarter);
            updateWrapper2.eq("lvl4Code", dsmCode);
            int insertCount2 = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper2);

            /**更新审批意见*/
            if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {
                CuspostQuarterRetailAddDsm cuspostRetailApplyQuarterInfo = new CuspostQuarterRetailAddDsm();
                UpdateWrapper<CuspostQuarterRetailAddDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterRetailAddDsmMapper.update(cuspostRetailApplyQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {
                CuspostQuarterRetailChangeDsm cuspostRetailChangeDeletionQuarterInfo = new CuspostQuarterRetailChangeDsm();
                UpdateWrapper<CuspostQuarterRetailChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterRetailChangeDsmMapper.update(cuspostRetailChangeDeletionQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 退回大区修改（商务打单商）
     */
    @ApiOperation(value = "退回大区修改（商务打单商）", notes = "退回大区修改（商务打单商）")
    @RequestMapping(value = "/sendBackDistributorQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper sendBackDistributorQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String dsmCode = object.getString("dsmCode"); // 申请人编码：dsmCode
            String sendBackInstruction = object.getString("sendBackInstruction"); // 退回说明：sendBackInstruction

            //更新申请编码状态 cuspost_quarter_apply_state_info
            CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
            UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("distributorApplyStateCode", UserConstant.APPLY_STATE_CODE_3);
            updateWrapper2.eq("typeCode", typeCode);
            updateWrapper2.eq("manageYear", manageYear);
            updateWrapper2.eq("manageQuarter", manageQuarter);
            updateWrapper2.eq("lvl4Code", dsmCode);
            int insertCount2 = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper2);

            /**更新审批意见*/
            if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {
                CuspostQuarterDistributorAddDsm cuspostDistributorApplyQuarterInfo = new CuspostQuarterDistributorAddDsm();
                UpdateWrapper<CuspostQuarterDistributorAddDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterDistributorAddDsmMapper.update(cuspostDistributorApplyQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {
                CuspostQuarterDistributorChangeDsm cuspostDistributorChangeDeletionQuarterInfo = new CuspostQuarterDistributorChangeDsm();
                UpdateWrapper<CuspostQuarterDistributorChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterDistributorChangeDsmMapper.update(cuspostDistributorChangeDeletionQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 退回大区修改（连锁总部）
     */
    @ApiOperation(value = "退回大区修改（连锁总部）", notes = "退回大区修改（连锁总部）")
    @RequestMapping(value = "/sendBackChainstoreHqQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper sendBackChainstoreHqQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String dsmCode = object.getString("dsmCode"); // 申请人编码：dsmCode
            String sendBackInstruction = object.getString("sendBackInstruction"); // 退回说明：sendBackInstruction

            //更新申请编码状态 cuspost_quarter_apply_state_info
            CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
            UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("chainstoreHqApplyStateCode", UserConstant.APPLY_STATE_CODE_3);
            updateWrapper2.eq("typeCode", typeCode);
            updateWrapper2.eq("manageYear", manageYear);
            updateWrapper2.eq("manageQuarter", manageQuarter);
            updateWrapper2.eq("lvl4Code", dsmCode);
            int insertCount2 = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper2);

            /**更新审批意见*/
            if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {
                CuspostQuarterChainstoreHqAddDsm cuspostChainstoreHqApplyQuarterInfo = new CuspostQuarterChainstoreHqAddDsm();
                UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterChainstoreHqAddDsmMapper.update(cuspostChainstoreHqApplyQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {
                CuspostQuarterChainstoreHqChangeDsm cuspostChainstoreHqChangeDeletionQuarterInfo = new CuspostQuarterChainstoreHqChangeDsm();
                UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_3);
                updateWrapper3.set("approvalOpinion", sendBackInstruction);
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl4Code", dsmCode);
                int insertCount3 = cuspostQuarterChainstoreHqChangeDsmMapper.update(cuspostChainstoreHqChangeDeletionQuarterInfo, updateWrapper3);

                if (insertCount2 <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }


    /**
     * 查询季度商务打单商申请数据
     */
    @ApiOperation(value = "查询季度商务打单商申请数据", notes = "查询季度商务打单商申请数据")
    @RequestMapping(value = "/queryDistributorApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryDistributorApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String propertyCode = object.getString("propertyCode"); // 属性
            String postCode = object.getString("postCode"); // postCode
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryDistributorApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, propertyCode
                    , region
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载季度商务打单商申请数据
     */
    @ApiOperation(value = "下载季度商务打单商申请数据", notes = "下载季度商务打单商申请数据")
    @RequestMapping(value = "/exprotDistributorApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotDistributorApplyQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String propertyCode = object.getString("propertyCode"); // 属性
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryDistributorApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, propertyCode
                    , region
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DISTRIBUTOR_ADD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度商务打单商申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * @MethodName 下载季度商务打单商申请数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度商务打单商申请数据 D&A下载按照上传模板顺序", notes = "下载季度商务打单商申请数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotDistributorApplyQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotDistributorApplyQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String propertyCode = object.getString("propertyCode"); // 属性
            String region = object.getString("region"); // region
            String postCode = object.getString("postCode"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryDistributorApplyQuarterInfo(page
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city, propertyCode
                    , region
                    , orderName
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_DISTRIBUTOR_APPLY_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度商务打单商申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 新增季度商务打单商申请数据
     */
    @ApiOperation(value = "新增季度商务打单商申请数据", notes = "新增季度商务打单商申请数据")
    @RequestMapping(value = "/addDistributorApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper addDistributorApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                     // 省份
            String city = object.getString("city");                             // 城市
            String address = object.getString("address");                               // 地址
            String propertyCode = object.getString("propertyCode");           // 属性
            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                   // postCode
            String region = object.getString("region");                   // region
            //20230529 START
            String telephone = object.getString("telephone");                   // 经销商电话
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String territoryDsmMp = object.getString("territoryDsmMp");                   // 联系电话
            String territoryDsmName = object.getString("territoryDsmName");                   // 负责人姓名
            String contactsAddress = object.getString("contactsAddress");                   // 经销商地址
            String contactsNamenphone = object.getString("contactsNamenphone");                   // 电话/人
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());

            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }


            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryDistributorDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, "");
            int count3 = customerPostMapper.queryDistributorDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, "");
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            /**创建applyCode申请编码*/
//            String applyCodeStr = this.getApplyCode(manageYear, manageQuarter);
//            if (StringUtils.isEmpty(applyCodeStr)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "错误", "申请编码没有数据！");
//            }
            String applyCodeStr = commonUtils.createUUID().replaceAll("-", "");// 20230424 新增申请编码

            /**插入数据表*/
            if (UserConstant.POST_CODE1.equals(postCode)) {
                CuspostQuarterDistributorAddDsm info = new CuspostQuarterDistributorAddDsm();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("商务/打单商");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setPropertyCode(propertyCode);
                info.setDsmCode(dsmCode);
                info.setDsmCwid(dsmCwid);
                info.setDsmName(dsmName);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTelephone(telephone);
                info.setTerritoryProducts(territoryProducts);
                info.setTerritoryDsmMp(territoryDsmMp);
                info.setTerritoryDsmName(territoryDsmName);
                info.setContactsAddress(contactsAddress);
                info.setContactsNamenphone(contactsNamenphone);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterDistributorAddDsmMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }

            if (UserConstant.POST_CODE2.equals(postCode)) {
                CuspostQuarterDistributorAddAssistant info = new CuspostQuarterDistributorAddAssistant();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("商务/打单商");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setPropertyCode(propertyCode);
                info.setDsmCode(dsmCode);
                info.setDsmCwid(dsmCwid);
                info.setDsmName(dsmName);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTelephone(telephone);
                info.setTerritoryProducts(territoryProducts);
                info.setTerritoryDsmMp(territoryDsmMp);
                info.setTerritoryDsmName(territoryDsmName);
                info.setContactsAddress(contactsAddress);
                info.setContactsNamenphone(contactsNamenphone);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterDistributorAddAssistantMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 更新季度商务打单商申请数据
     */
    @ApiOperation(value = "修改季度商务打单商申请数据", notes = "更新季度商务打单商申请数据")
    @RequestMapping(value = "/updateDistributorApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper updateDistributorApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int autoKey = object.getInteger("autoKey");                                   // autoKey
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String applyCode = object.getString("applyCode");                  // 申请编码
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                     // 省份
            String city = object.getString("city");                             // 城市
            String address = object.getString("address");                               // 地址
            String propertyCode = object.getString("propertyCode");           // 属性
            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                   // postCode
            //20230529 START
            String telephone = object.getString("telephone");                   // 经销商电话
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String territoryDsmMp = object.getString("territoryDsmMp");                   // 联系电话
            String territoryDsmName = object.getString("territoryDsmName");                   // 负责人姓名
            String contactsAddress = object.getString("contactsAddress");                   // 经销商地址
            String contactsNamenphone = object.getString("contactsNamenphone");                   // 电话/人
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryDistributorDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, applyCode);
            int count3 = customerPostMapper.queryDistributorDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, applyCode);
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterDistributorAddDsm info = cuspostQuarterDistributorAddDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterDistributorAddDsm>()
                                .eq("applyCode", applyCode)
                );

                if (StringUtils.isEmpty(info)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    //地区经理只能改自己的，看不见大区助理的内容，这块查不到就是错了
                }
                /**更新*/
                UpdateWrapper<CuspostQuarterDistributorAddDsm> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("customerName", customerName);
                updateWrapper.set("province", province);
                updateWrapper.set("city", city);
                updateWrapper.set("address", address);
                updateWrapper.set("propertyCode", propertyCode);
                updateWrapper.set("dsmCode", dsmCode);
                updateWrapper.set("dsmCwid", dsmCwid);
                updateWrapper.set("dsmName", dsmName);
                updateWrapper.set("channelRemark", channelRemark);
                //20230529 START
                updateWrapper.set("telephone", telephone);
                updateWrapper.set("territoryProducts", territoryProducts);
                updateWrapper.set("territoryDsmMp", territoryDsmMp);
                updateWrapper.set("territoryDsmName", territoryDsmName);
                updateWrapper.set("contactsAddress", contactsAddress);
                updateWrapper.set("contactsNamenphone", contactsNamenphone);
                //20230529 END
                updateWrapper.set("updateTime", new Date());
                updateWrapper.set("updateUser", loginUser.getUserCode());
                updateWrapper.eq("applyCode", applyCode);
                int insertCount = cuspostQuarterDistributorAddDsmMapper.update(info, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterDistributorAddAssistant infoAssi = cuspostQuarterDistributorAddAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterDistributorAddAssistant>()
                                .eq("applyCode", applyCode)
                );
                if (StringUtils.isEmpty(infoAssi)) {
                    //如果不是大区助理的内容就去找地区经理
                    //获取既存数据
                    CuspostQuarterDistributorAddDsm infoDsm = cuspostQuarterDistributorAddDsmMapper.selectOne(
                            new QueryWrapper<CuspostQuarterDistributorAddDsm>()
                                    .eq("applyCode", applyCode)
                    );
                    if (StringUtils.isEmpty(infoDsm)) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    } else {
                        CuspostQuarterDistributorAddAssistant infoAssiInsert = new CuspostQuarterDistributorAddAssistant();
                        infoAssiInsert.setApplyCode(infoDsm.getApplyCode());
                        infoAssiInsert.setManageYear(BigDecimal.valueOf(manageYear));
                        infoAssiInsert.setManageQuarter(manageQuarter);
                        infoAssiInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                        infoAssiInsert.setCustomerTypeName("零售终端");//客户类型
                        infoAssiInsert.setRegion(infoDsm.getLvl2Code());//大区
                        infoAssiInsert.setCustomerName(customerName);
                        infoAssiInsert.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                        infoAssiInsert.setProvince(province);
                        infoAssiInsert.setCity(city);
                        infoAssiInsert.setAddress(address);
                        infoAssiInsert.setPropertyCode(propertyCode);
                        infoAssiInsert.setDsmCode(dsmCode);
                        infoAssiInsert.setDsmCwid(dsmCwid);
                        infoAssiInsert.setDsmName(dsmName);
                        infoAssiInsert.setChannelRemark(channelRemark);
                        infoAssiInsert.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                        infoAssiInsert.setLvl2Code(infoDsm.getLvl2Code());
//                        infoAssiInsert.setLvl3Code(infoDsm.getLvl3Code());
                        infoAssiInsert.setLvl3Code(null);
                        infoAssiInsert.setLvl4Code(infoDsm.getLvl4Code());
                        //20230529 START
                        infoAssiInsert.setTelephone(telephone);
                        infoAssiInsert.setTerritoryProducts(territoryProducts);
                        infoAssiInsert.setTerritoryDsmMp(territoryDsmMp);
                        infoAssiInsert.setTerritoryDsmName(territoryDsmName);
                        infoAssiInsert.setContactsAddress(contactsAddress);
                        infoAssiInsert.setContactsNamenphone(contactsNamenphone);
                        //20230529 END

                        int insertCount = cuspostQuarterDistributorAddAssistantMapper.insert(infoAssiInsert);
                        if (insertCount <= 0) {
                            return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                        }
                    }
                } else {
                    /**更新*/
                    UpdateWrapper<CuspostQuarterDistributorAddAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("province", province);
                    updateWrapper.set("city", city);
                    updateWrapper.set("address", address);
                    updateWrapper.set("propertyCode", propertyCode);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("channelRemark", channelRemark);
                    //20230529 START
                    updateWrapper.set("telephone", telephone);
                    updateWrapper.set("territoryProducts", territoryProducts);
                    updateWrapper.set("territoryDsmMp", territoryDsmMp);
                    updateWrapper.set("territoryDsmName", territoryDsmName);
                    updateWrapper.set("contactsAddress", contactsAddress);
                    updateWrapper.set("contactsNamenphone", contactsNamenphone);
                    //20230529 END
                    updateWrapper.set("updateTime", new Date());
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.eq("applyCode", applyCode);
                    int insertCount = cuspostQuarterDistributorAddAssistantMapper.update(infoAssi, updateWrapper);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                    }
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 删除季度商务打单商申请数据
     */
    @ApiOperation(value = "删除季度商务打单商申请数据", notes = "删除季度商务打单商申请数据")
    @RequestMapping(value = "/deleteDistributorApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper deleteDistributorApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String autoKey = object.getString("autoKey");                               // autoKey
            String applyCode = object.getString("applyCode");                               // 申请编码
            String postCode = object.getString("postCode");                               // postCode

            // 必须检查
            if (StringUtils.isEmpty(autoKey) || StringUtils.isEmpty(applyCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterDistributorAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterDistributorAddDsmMapper.delete(updateWrapper1);

            UpdateWrapper<CuspostQuarterDistributorAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterDistributorAddAssistantMapper.delete(updateWrapper2);
            if (insertCount1 <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据删除失败！");
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 上传季度商务打单商申请数据
     */
    @ApiOperation(value = "上传季度商务打单商申请数据", notes = "上传季度商务打单商申请数据")
    @RequestMapping("/batchAddDistributorApplyQuarterInfo")
    @Transactional
    public Wrapper batchAddDistributorApplyQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DISTRIBUTOR_ADD);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_distributor_add_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_distributor_add_assistant";
                }
                String errorFileName = distributorApplyQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String distributorApplyQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 地区大区表查重（临时表已经有数据，check客户名称是否在地区经理表，大区助理表，核心表中存在，流向年月+客户名称）
            String cusNames1 = customerPostMapper.queryDistributorDsmCusDuplicateFromSelfByTon(
                    tableEnNameTem, manageYear, manageQuarter, fileId);
            if (cusNames1 != null) {
                String messageContent = " 客户名称 【" + cusNames1 + "】在本季新增中已存在，请确认！";
                errorMessageList.add(messageContent);
            }
            //check 核心表
            String cusNames2 = customerPostMapper.queryDistributorDsmCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR, fileId, UserConstant.APPLY_TYPE_CODE1);
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR, fileId, UserConstant.APPLY_TYPE_CODE1);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                /**获取上传数*/
                int count = customerPostMapper.queryCountForUpload(UserConstant.UPLOAD_TABLE_PREFIX + tableEnName, fileId);

                /**创建applyCode申请编码*/
//                int applyCodeInt = this.getApplyCodeBatch(manageYear, manageQuarter, count);

                //更新 终端类型，大区，postCode，lvl2Code，lvl3Code，lvl4Code
                customerPostMapper.uploadDistributorApplyQuarterInfoOther(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth, nowYM
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);

                // 更新上传数据 页面可以修改 影响applyCode批量创建
                // 插入上传数据
//                customerPostMapper.uploadDistributorApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode, applyCodeInt);
                customerPostMapper.uploadDistributorApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode);// 20230424 新增申请编码
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }


    /**
     * 提交季度商务打单商申请数据
     */
    @ApiOperation(value = "提交季度商务打单商申请数据", notes = "提交季度商务打单商申请数据")
    @RequestMapping(value = "/submitDistributorApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper submitDistributorApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String customerTypeCode = object.getString("customerTypeCode"); // 1医院,2零售,3商务,4连锁
            String applyJudge = object.getString("applyJudge"); // 大于等于小于
            String postCode = object.getString("postCode"); // 岗位编码，1：地区经理，2：大区助理
            String assistantRemark = object.getString("assistantRemark"); // 大区助理备注
            String region = object.getString("region"); // 商务大区

            // 必须检查
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "商务大区不可以为空！");
//            }
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**地区经理提交 cuspost_quarter_apply_state_info*/
            String approver = "";//当前审批人
            String buttonEffect = "";//按钮是否有效

            if (UserConstant.POST_CODE1.equals(postCode) && UserConstant.CUSTOMER_TYPE_DISTRIBUTOR.equals(customerTypeCode)) {
//                /**获取大区，地区等岗位编码*/
////                String lvl2Code = "";
////                String lvl3Code = "";
//                String lvl4Code = "";
//                String assistantName = "";
//                String lvl3Name = "";
////                List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
//                if (lvlList.size() > 0) {
////                    lvl2Code = lvlList.get(0).getLvl2Code();
////                    lvl3Code = lvlList.get(0).getLvl3Code();
//                    lvl4Code = lvlList.get(0).getLvl4Code();
//                    assistantName = lvlList.get(0).getAssistantName();
//                    lvl3Name = lvlList.get(0).getLvl3Name();
//                } else {
//                    //架构错误
//                }

                //地区经理查询自己的数据
                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    List<CuspostQuarterDistributorAddDsm> existList = cuspostQuarterDistributorAddDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterDistributorAddDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    List<CuspostQuarterDistributorChangeDsm> existList = cuspostQuarterDistributorChangeDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterDistributorChangeDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }

                //按钮是否有效
                buttonEffect = "distributorButtonEffect";
                //获取审批人
//                approver = assistantName;
                approver = "大区助理";

                //更新申请编码状态 cuspost_quarter_apply_state_info
                CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("distributorApplyStateCode", UserConstant.APPLY_STATE_CODE_2);
                updateWrapper.eq("typeCode", typeCode);
                updateWrapper.eq("manageYear", manageYear);
                updateWrapper.eq("manageQuarter", manageQuarter);
                updateWrapper.eq("lvl4Code", lvl4Code);
                int insertCount = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                }

                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    //更新申请编码状态 cuspost_quarter_distributor_add_dsm
                    CuspostQuarterDistributorAddDsm cuspostDistributorApplyQuarterInfo = new CuspostQuarterDistributorAddDsm();
                    UpdateWrapper<CuspostQuarterDistributorAddDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterDistributorAddDsmMapper.update(cuspostDistributorApplyQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    //更新申请编码状态 cuspost_quarter_distributor_change_dsm
                    CuspostQuarterDistributorChangeDsm cuspostDistributorChangeDeletionQuarterInfo = new CuspostQuarterDistributorChangeDsm();
                    UpdateWrapper<CuspostQuarterDistributorChangeDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterDistributorChangeDsmMapper.update(cuspostDistributorChangeDeletionQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
            }

            /**大区助理提交
             * cuspost_quarter_distributor_add_dsm
             * cuspost_quarter_distributor_add_assistant
             * cuspost_quarter_distributor_change_dsm
             * cuspost_quarter_distributor_change_assistant
             * cuspost_quarter_apply_state_info
             * cuspost_quarter_apply_state_region_info
             * */
            if (UserConstant.POST_CODE2.equals(postCode) && UserConstant.CUSTOMER_TYPE_DISTRIBUTOR.equals(customerTypeCode)) {
                //大区助理查询地区和大区的数据
                List<CuspostQuarterDistributorAddDsm> existList1 = cuspostQuarterDistributorAddDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterDistributorAddDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
//                                .eq("lvl2Code", lvl2Code)
                                .eq("lvl2Code", region)
                );
                List<CuspostQuarterDistributorAddAssistant> existList2 = cuspostQuarterDistributorAddAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterDistributorAddAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
//                                .eq("lvl2Code", lvl2Code)
                                .eq("lvl2Code", region)
                );
                List<CuspostQuarterDistributorChangeDsm> existList3 = cuspostQuarterDistributorChangeDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterDistributorChangeDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
//                                .eq("lvl2Code", lvl2Code)
                                .eq("lvl2Code", region)
                );
                List<CuspostQuarterDistributorChangeAssistant> existList4 = cuspostQuarterDistributorChangeAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterDistributorChangeAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
//                                .eq("lvl2Code", lvl2Code)
                                .eq("lvl2Code", region)
                );
                if ((StringUtils.isEmpty(existList1) || existList1.size() < 1)
                        && (StringUtils.isEmpty(existList2) || existList2.size() < 1)
                        && (StringUtils.isEmpty(existList3) || existList3.size() < 1)
                        && (StringUtils.isEmpty(existList4) || existList4.size() < 1)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                }

                //按钮是否有效
                buttonEffect = "distributorButtonEffect";
                //查询审批人：approver
//                approver = lvl3Name;
                approver = "大区总监";

                /**更新申请编码状态 cuspost_quarter_distributor_add_dsm*/
                CuspostQuarterDistributorAddDsm info1 = new CuspostQuarterDistributorAddDsm();
                UpdateWrapper<CuspostQuarterDistributorAddDsm> updateWrapper1 = new UpdateWrapper<>();
                updateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper1.set("approver", approver);
                updateWrapper1.set("verifyRemark", assistantRemark);
                updateWrapper1.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper1.eq("manageYear", manageYear);
                updateWrapper1.eq("manageQuarter", manageQuarter);
//                updateWrapper1.eq("lvl2Code", lvl2Code);
                updateWrapper1.eq("lvl2Code", region);
                int insertCount1 = cuspostQuarterDistributorAddDsmMapper.update(info1, updateWrapper1);

                /**更新申请编码状态 cuspost_quarter_distributor_add_assistant*/
                CuspostQuarterDistributorAddAssistant info2 = new CuspostQuarterDistributorAddAssistant();
                UpdateWrapper<CuspostQuarterDistributorAddAssistant> updateWrapper2 = new UpdateWrapper<>();
                updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper2.set("approver", approver);
                updateWrapper2.set("verifyRemark", assistantRemark);
                updateWrapper2.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper2.eq("manageYear", manageYear);
                updateWrapper2.eq("manageQuarter", manageQuarter);
//                updateWrapper2.eq("lvl2Code", lvl2Code);
                updateWrapper2.eq("lvl2Code", region);
                int insertCount2 = cuspostQuarterDistributorAddAssistantMapper.update(info2, updateWrapper2);

                /**更新申请编码状态 cuspost_quarter_distributor_change_dsm*/
                CuspostQuarterDistributorChangeDsm info3 = new CuspostQuarterDistributorChangeDsm();
                UpdateWrapper<CuspostQuarterDistributorChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper3.set("approver", approver);
                updateWrapper3.set("verifyRemark", assistantRemark);
                updateWrapper3.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
//                updateWrapper3.eq("lvl2Code", lvl2Code);
                updateWrapper3.eq("lvl2Code", region);
                int insertCount3 = cuspostQuarterDistributorChangeDsmMapper.update(info3, updateWrapper3);

                /**更新申请编码状态 cuspost_quarter_distributor_change_assistant*/
                CuspostQuarterDistributorChangeAssistant info4 = new CuspostQuarterDistributorChangeAssistant();
                UpdateWrapper<CuspostQuarterDistributorChangeAssistant> updateWrapper4 = new UpdateWrapper<>();
                updateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper4.set("approver", approver);
                updateWrapper4.set("verifyRemark", assistantRemark);
                updateWrapper4.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper4.eq("manageYear", manageYear);
                updateWrapper4.eq("manageQuarter", manageQuarter);
//                updateWrapper4.eq("lvl2Code", lvl2Code);
                updateWrapper4.eq("lvl2Code", region);
                int insertCount4 = cuspostQuarterDistributorChangeAssistantMapper.update(info4, updateWrapper4);

                /**更新申请编码状态 cuspost_quarter_apply_state_info*/
                CuspostQuarterApplyStateInfo info5 = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper5 = new UpdateWrapper<>();
                updateWrapper5.set("distributorApplyStateCode", UserConstant.APPLY_STATE_CODE_4);
                //updateWrapper5.eq("typeCode", typeCode); 大区助理提交时，新增和变更删除都一起提交
                updateWrapper5.eq("manageYear", manageYear);
                updateWrapper5.eq("manageQuarter", manageQuarter);
//                updateWrapper5.eq("lvl2Code", lvl2Code);
                updateWrapper5.eq("lvl2Code", region);
                int insertCount5 = cuspostQuarterApplyStateInfoMapper.update(info5, updateWrapper5);

                //20230613 没有数据的状态变为已完成 START
                customerPostMapper.updateQuarterApplyStateDistributorAddAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateDistributorAddDsmNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateDistributorChangeAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateDistributorChangeDsmNoData(manageYear, manageQuarter, lvl2Code);
                //20230613 没有数据的状态变为已完成 END

                /**更新申请编码状态 cuspost_quarter_apply_state_region_info*/
                CuspostQuarterApplyStateRegionInfo insertModel = new CuspostQuarterApplyStateRegionInfo();
                insertModel.setManageYear(BigDecimal.valueOf(manageYear));
                insertModel.setManageQuarter(manageQuarter);
                insertModel.setCustomerTypeCode(customerTypeCode);
//                insertModel.setRegion(lvl2Code);
                insertModel.setRegion(region);
                insertModel.setApplyStateCode(UserConstant.APPLY_STATE_CODE_4);
                insertModel.setPostCode(UserConstant.POST_CODE3);
                insertModel.setIsOver("0");
                if ("大于".equals(applyJudge)) {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A004");
                    } else {
                        insertModel.setApprovalProcessCode("A002");
                    }

                } else {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A003");
                    } else {
                        insertModel.setApprovalProcessCode("A001");
                    }
                }
                //20230613 如果没有数据不进入到总监审批 START
                if ((!StringUtils.isEmpty(existList1) && existList1.size() > 0)
                        || (!StringUtils.isEmpty(existList2) && existList2.size() > 0)) {
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE1);
                    int insertCountInsert1 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                if ((!StringUtils.isEmpty(existList3) && existList3.size() > 0)
                        || (!StringUtils.isEmpty(existList4) && existList4.size() > 0)) {
                    //变更删除的场合再创建一遍
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE2);
                    int insertCountInsert2 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                //20230613 如果没有数据不进入到总监审批 END
            }
            resultMap.put(buttonEffect, "0"); //按钮不可用

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 查询季度连锁总部申请数据
     */
    @ApiOperation(value = "查询季度连锁总部申请数据", notes = "查询季度连锁总部申请数据")
    @RequestMapping(value = "/queryChainstoreHqApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryChainstoreHqApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String postCode = object.getString("postCode"); // postCode
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryChainstoreHqApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city
                    , region
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载季度连锁总部申请数据
     */
    @ApiOperation(value = "下载季度连锁总部申请数据", notes = "下载季度连锁总部申请数据")
    @RequestMapping(value = "/exprotChainstoreHqApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotChainstoreHqApplyQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String postCode = object.getString("postCode"); // postCode
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryChainstoreHqApplyQuarterInfo(page
//                    , postCode, lvl2Code, lvl4Code
//                    , postCode, lvl2Codes, lvl4Code
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city
                    , region
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_CHAINSTORE_HQ_ADD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度连锁总部申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * @MethodName 下载季度连锁总部申请数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度连锁总部申请数据 D&A下载按照上传模板顺序", notes = "下载季度连锁总部申请数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotChainstoreHqApplyQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotChainstoreHqApplyQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String postCode = object.getString("postCode"); // postCode
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryChainstoreHqApplyQuarterInfo(page
                    , postCode, lvl4Code
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city
                    , region
                    , orderName
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_CHAINSTORE_HQ_APPLY_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度连锁总部申请数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 新增季度连锁总部申请数据
     */
    @ApiOperation(value = "新增季度连锁总部申请数据", notes = "新增季度连锁总部申请数据")
    @RequestMapping(value = "/addChainstoreHqApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper addChainstoreHqApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                     // 省份
            String city = object.getString("city");                             // 城市
            String address = object.getString("address");                               // 地址
            String printListConditionCode = object.getString("printListConditionCode"); // 打单情况
            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String kaTerritoryKaCode = object.getString("kaTerritoryKaCode");           // KA负责人岗位
            String kaTerritoryExpandCode = object.getString("kaTerritoryExpandCode");   // KA拓展经理岗位
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                   // postCode
            String region = object.getString("region");                   // region
            //20230529 START
            String telephone = object.getString("telephone");                   // 电话
            String kaUpStreamLeChoId = object.getString("kaUpStreamLeChoId");                   // 归属上级编码
            String kaUpStreamLeName = object.getString("kaUpStreamLeName");                   // 归属上级名称
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String territoryTuozhanCountby = object.getString("territoryTuozhanCountby");       // 计量方式
            String gongcangHcoId = object.getString("gongcangHcoId");                   // 共仓打单商业代码
            String gongcangName = object.getString("gongcangName");                   // 共仓打单商业名称
            String gongcangGrade = object.getString("gongcangGrade");                   // 共仓商业级别
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());

            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }

            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, "");
            int count3 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, "");
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }


            /**创建applyCode申请编码*/
//            String applyCodeStr = this.getApplyCode(manageYear, manageQuarter);
//            if (StringUtils.isEmpty(applyCodeStr)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "错误", "申请编码没有数据！");
//            }
            String applyCodeStr = commonUtils.createUUID().replaceAll("-", "");// 20230424 新增申请编码

            /**插入数据表*/
            if (UserConstant.POST_CODE1.equals(postCode)) {
                CuspostQuarterChainstoreHqAddDsm info = new CuspostQuarterChainstoreHqAddDsm();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("连锁总部");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setPrintListConditionCode(printListConditionCode);
                info.setDsmCode(dsmCode);
                info.setDsmCwid(dsmCwid);
                info.setDsmName(dsmName);
                info.setKaTerritoryKaCode(kaTerritoryKaCode);
                info.setKaTerritoryExpandCode(kaTerritoryExpandCode);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTelephone(telephone);
                info.setKaUpStreamLeChoId(kaUpStreamLeChoId);
                info.setKaUpStreamLeName(kaUpStreamLeName);
                info.setTerritoryProducts(territoryProducts);
                info.setTerritoryTuozhanCountby(territoryTuozhanCountby);
                info.setGongcangHcoId(gongcangHcoId);
                info.setGongcangName(gongcangName);
                info.setGongcangGrade(gongcangGrade);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterChainstoreHqAddDsmMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }

            if (UserConstant.POST_CODE2.equals(postCode)) {
                CuspostQuarterChainstoreHqAddAssistant info = new CuspostQuarterChainstoreHqAddAssistant();
                info.setApplyCode(applyCodeStr);
                info.setManageYear(BigDecimal.valueOf(manageYear));
                info.setManageQuarter(manageQuarter);
                info.setYearMonth(BigDecimal.valueOf(manageMonth));
                info.setCustomerTypeName("连锁总部");//客户类型
                info.setRegion(lvl2Code);//大区
//                info.setRegion(region);//大区
                info.setCustomerName(customerName);
                info.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                info.setProvince(province);
                info.setCity(city);
                info.setAddress(address);
                info.setPrintListConditionCode(printListConditionCode);
                info.setDsmCode(dsmCode);
                info.setDsmCwid(dsmCwid);
                info.setDsmName(dsmName);
                info.setKaTerritoryKaCode(kaTerritoryKaCode);
                info.setKaTerritoryExpandCode(kaTerritoryExpandCode);
                info.setChannelRemark(channelRemark);
                info.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                info.setLvl2Code(lvl2Code);
//                info.setLvl2Code(region);
//                info.setLvl3Code(lvl3Code);
                info.setLvl3Code(null);
                info.setLvl4Code(lvl4Code);
                //20230529 START
                info.setTelephone(telephone);
                info.setKaUpStreamLeChoId(kaUpStreamLeChoId);
                info.setKaUpStreamLeName(kaUpStreamLeName);
                info.setTerritoryProducts(territoryProducts);
                info.setTerritoryTuozhanCountby(territoryTuozhanCountby);
                info.setGongcangHcoId(gongcangHcoId);
                info.setGongcangName(gongcangName);
                info.setGongcangGrade(gongcangGrade);
                //20230529 END
                //20230625 START
                info.setInsertUser(loginUser.getUserCode());
                info.setInsertTime(new Date());
                //20230625 END

                int insertCount = cuspostQuarterChainstoreHqAddAssistantMapper.insert(info);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 更新季度连锁总部申请数据
     */
    @ApiOperation(value = "修改季度连锁总部申请数据", notes = "更新季度连锁总部申请数据")
    @RequestMapping(value = "/updateChainstoreHqApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper updateChainstoreHqApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int autoKey = object.getInteger("autoKey");                                   // autoKey
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String applyCode = object.getString("applyCode");                  // 申请编码
            String customerName = object.getString("customerName");                     // 客户名称
            String province = object.getString("province");                     // 省份
            String city = object.getString("city");                             // 城市
            String address = object.getString("address");                               // 地址
            String printListConditionCode = object.getString("printListConditionCode"); // 打单情况
            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String kaTerritoryKaCode = object.getString("kaTerritoryKaCode");           // KA负责人岗位
            String kaTerritoryExpandCode = object.getString("kaTerritoryExpandCode");   // KA拓展经理岗位
            String channelRemark = object.getString("channelRemark");                   // 渠道备注
            String postCode = object.getString("postCode");                   // postCode
            String region = object.getString("region");                   // region
            //20230529 START
            String telephone = object.getString("telephone");                   // 电话
            String kaUpStreamLeChoId = object.getString("kaUpStreamLeChoId");                   // 归属上级编码
            String kaUpStreamLeName = object.getString("kaUpStreamLeName");                   // 归属上级名称
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String territoryTuozhanCountby = object.getString("territoryTuozhanCountby");       // 计量方式
            String gongcangHcoId = object.getString("gongcangHcoId");                   // 共仓打单商业代码
            String gongcangName = object.getString("gongcangName");                   // 共仓打单商业名称
            String gongcangGrade = object.getString("gongcangGrade");                   // 共仓商业级别
            //20230529 END

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(province) || StringUtils.isEmpty(city) || StringUtils.isEmpty(address)
                    || StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            /**校验 客户名称，与已提交的未删除或未驳回名称进行查重，与已有主数据进行查重*/
            //自己的数据进行查重
            int count1 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromSelf(manageYear, manageQuarter, customerName, applyCode);
            int count3 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromSelf2(manageYear, manageQuarter, customerName, applyCode);
            if (count1 > 0 || count3 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与申请主数据重复！");
            }

            //与已有主数据进行查重
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
//            int count2 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, null, customerName);
            int count2 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
            if (count2 > 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "与已有主数据重复！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());

            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterChainstoreHqAddDsm info = cuspostQuarterChainstoreHqAddDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterChainstoreHqAddDsm>()
                                .eq("applyCode", applyCode)
                );

                if (StringUtils.isEmpty(info)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    //地区经理只能改自己的，看不见大区助理的内容，这块查不到就是错了
                }
                /**更新*/
                UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("customerName", customerName);
                updateWrapper.set("province", province);
                updateWrapper.set("city", city);
                updateWrapper.set("address", address);
                updateWrapper.set("printListConditionCode", printListConditionCode);
                updateWrapper.set("dsmCode", dsmCode);
                updateWrapper.set("dsmCwid", dsmCwid);
                updateWrapper.set("dsmName", dsmName);
                updateWrapper.set("kaTerritoryKaCode", kaTerritoryKaCode);
                updateWrapper.set("kaTerritoryExpandCode", kaTerritoryExpandCode);
                updateWrapper.set("channelRemark", channelRemark);
                //20230529 START
                updateWrapper.set("telephone", telephone);
                updateWrapper.set("kaUpStreamLeChoId", kaUpStreamLeChoId);
                updateWrapper.set("kaUpStreamLeName", kaUpStreamLeName);
                updateWrapper.set("territoryProducts", territoryProducts);
                updateWrapper.set("territoryTuozhanCountby", territoryTuozhanCountby);
                updateWrapper.set("gongcangHcoId", gongcangHcoId);
                updateWrapper.set("gongcangName", gongcangName);
                updateWrapper.set("gongcangGrade", gongcangGrade);
                //20230529 END
                updateWrapper.set("updateTime", new Date());
                updateWrapper.set("updateUser", loginUser.getUserCode());
                updateWrapper.eq("applyCode", applyCode);
                int insertCount = cuspostQuarterChainstoreHqAddDsmMapper.update(info, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                }
            }
            if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterChainstoreHqAddAssistant infoAssi = cuspostQuarterChainstoreHqAddAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterChainstoreHqAddAssistant>()
                                .eq("applyCode", applyCode)
                );
                if (StringUtils.isEmpty(infoAssi)) {
                    //如果不是大区助理的内容就去找地区经理
                    //获取既存数据
                    CuspostQuarterChainstoreHqAddDsm infoDsm = cuspostQuarterChainstoreHqAddDsmMapper.selectOne(
                            new QueryWrapper<CuspostQuarterChainstoreHqAddDsm>()
                                    .eq("applyCode", applyCode)
                    );
                    if (StringUtils.isEmpty(infoDsm)) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "该数据已经被删除！");
                    } else {
                        CuspostQuarterChainstoreHqAddAssistant infoAssiInsert = new CuspostQuarterChainstoreHqAddAssistant();
                        infoAssiInsert.setApplyCode(infoDsm.getApplyCode());
                        infoAssiInsert.setManageYear(BigDecimal.valueOf(manageYear));
                        infoAssiInsert.setManageQuarter(manageQuarter);
                        infoAssiInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                        infoAssiInsert.setCustomerTypeName("零售终端");//客户类型
                        infoAssiInsert.setRegion(infoDsm.getLvl2Code());//大区
                        infoAssiInsert.setCustomerName(customerName);
                        infoAssiInsert.setApplyStateCode(UserConstant.DETAIL_APPLY_STATE_CODE_1);
                        infoAssiInsert.setProvince(province);
                        infoAssiInsert.setCity(city);
                        infoAssiInsert.setAddress(address);
                        infoAssiInsert.setPrintListConditionCode(printListConditionCode);
                        infoAssiInsert.setDsmCode(dsmCode);
                        infoAssiInsert.setDsmCwid(dsmCwid);
                        infoAssiInsert.setDsmName(dsmName);
                        infoAssiInsert.setKaTerritoryKaCode(kaTerritoryKaCode);
                        infoAssiInsert.setKaTerritoryExpandCode(kaTerritoryExpandCode);
                        infoAssiInsert.setChannelRemark(channelRemark);
                        infoAssiInsert.setPostCode(postCode); // 岗位编码，1：地区经理，2：大区助理
                        infoAssiInsert.setLvl2Code(infoDsm.getLvl2Code());
//                        infoAssiInsert.setLvl3Code(infoDsm.getLvl3Code());
                        infoAssiInsert.setLvl3Code(null);
                        infoAssiInsert.setLvl4Code(infoDsm.getLvl4Code());
                        //20230529 START
                        infoAssiInsert.setTelephone(telephone);
                        infoAssiInsert.setKaUpStreamLeChoId(kaUpStreamLeChoId);
                        infoAssiInsert.setKaUpStreamLeName(kaUpStreamLeName);
                        infoAssiInsert.setTerritoryProducts(territoryProducts);
                        infoAssiInsert.setTerritoryTuozhanCountby(territoryTuozhanCountby);
                        infoAssiInsert.setGongcangHcoId(gongcangHcoId);
                        infoAssiInsert.setGongcangName(gongcangName);
                        infoAssiInsert.setGongcangGrade(gongcangGrade);
                        //20230529 END

                        int insertCount = cuspostQuarterChainstoreHqAddAssistantMapper.insert(infoAssiInsert);
                        if (insertCount <= 0) {
                            return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                        }
                    }
                } else {
                    /**更新*/
                    UpdateWrapper<CuspostQuarterChainstoreHqAddAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("province", province);
                    updateWrapper.set("city", city);
                    updateWrapper.set("address", address);
                    updateWrapper.set("printListConditionCode", printListConditionCode);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("kaTerritoryKaCode", kaTerritoryKaCode);
                    updateWrapper.set("kaTerritoryExpandCode", kaTerritoryExpandCode);
                    updateWrapper.set("channelRemark", channelRemark);
                    //20230529 START
                    updateWrapper.set("telephone", telephone);
                    updateWrapper.set("upStreamLeChoId", kaUpStreamLeChoId);
                    updateWrapper.set("upStreamLeName", kaUpStreamLeName);
                    updateWrapper.set("territoryProducts", territoryProducts);
                    updateWrapper.set("territoryTuozhanCountby", territoryTuozhanCountby);
                    updateWrapper.set("gongcangHcoId", gongcangHcoId);
                    updateWrapper.set("gongcangName", gongcangName);
                    updateWrapper.set("gongcangGrade", gongcangGrade);
                    //20230529 END
                    updateWrapper.set("updateTime", new Date());
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.eq("applyCode", applyCode);
                    int insertCount = cuspostQuarterChainstoreHqAddAssistantMapper.update(infoAssi, updateWrapper);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
                    }
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 删除季度连锁总部申请数据
     */
    @ApiOperation(value = "删除季度连锁总部申请数据", notes = "删除季度连锁总部申请数据")
    @RequestMapping(value = "/deleteChainstoreHqApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper deleteChainstoreHqApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String autoKey = object.getString("autoKey");                               // autoKey
            String applyCode = object.getString("applyCode");                               // 申请编码
            String postCode = object.getString("postCode");                               // postCode

            // 必须检查
            if (StringUtils.isEmpty(autoKey) || StringUtils.isEmpty(applyCode) || StringUtils.isEmpty(postCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterChainstoreHqAddDsmMapper.delete(updateWrapper1);

            UpdateWrapper<CuspostQuarterChainstoreHqAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterChainstoreHqAddAssistantMapper.delete(updateWrapper2);
            if (insertCount1 <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据删除失败！");
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 上传季度连锁总部申请数据
     */
    @ApiOperation(value = "上传季度连锁总部申请数据", notes = "上传季度连锁总部申请数据")
    @RequestMapping("/batchAddChainstoreHqApplyQuarterInfo")
    @Transactional
    public Wrapper batchAddChainstoreHqApplyQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_CHAINSTORE_HQ_ADD);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_chainstore_hq_add_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_chainstore_hq_add_assistant";
                }
                String errorFileName = chainstoreHqApplyQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String chainstoreHqApplyQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 地区大区表查重（临时表已经有数据，check客户名称是否在地区经理表，大区助理表，核心表中存在，流向年月+客户名称）
            String cusNames1 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromSelfByTon(
                    tableEnNameTem, manageYear, manageQuarter, fileId);
            if (cusNames1 != null) {
                String messageContent = " 客户名称 【" + cusNames1 + "】在本季新增中已存在，请确认！";
                errorMessageList.add(messageContent);
            }
            //check 核心表
            String cusNames2 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }


            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ, fileId, UserConstant.APPLY_TYPE_CODE1);
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ, fileId, UserConstant.APPLY_TYPE_CODE1);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                /**获取上传数*/
                int count = customerPostMapper.queryCountForUpload(UserConstant.UPLOAD_TABLE_PREFIX + tableEnName, fileId);

                /**创建applyCode申请编码*/
//                int applyCodeInt = this.getApplyCodeBatch(manageYear, manageQuarter, count);

                //更新 终端类型，大区，postCode，lvl2Code，lvl3Code，lvl4Code
                customerPostMapper.uploadChainstoreHqApplyQuarterInfoOther(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth, nowYM
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);

                // 更新上传数据 页面可以修改 影响applyCode批量创建
                // 插入上传数据
//                customerPostMapper.uploadChainstoreHqApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode, applyCodeInt);
                customerPostMapper.uploadChainstoreHqApplyQuarterInfoInsert(tableEnName, manageYear, manageQuarter, fileId, userCode);// 20230424 新增申请编码
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 提交季度连锁总部申请数据
     */
    @ApiOperation(value = "提交季度连锁总部申请数据", notes = "提交季度连锁总部申请数据")
    @RequestMapping(value = "/submitChainstoreHqApplyQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper submitChainstoreHqApplyQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String typeCode = object.getString("typeCode"); // 类型编码，1：新增，2：变更删除
            String customerTypeCode = object.getString("customerTypeCode"); // 1医院,2零售,3商务,4连锁
            String applyJudge = object.getString("applyJudge"); // 大于等于小于
            String postCode = object.getString("postCode"); // 岗位编码，1：地区经理，2：大区助理
            String assistantRemark = object.getString("assistantRemark"); // 大区助理备注
            String region = object.getString("region");                   // region

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCodeDsmAssistant(nowYM, postCode, loginUser.getUserCode());
//            String lvl2Code = "";
//            String lvl3Code = "";
//            String lvl4Code = "";
//            String assistantName = "";
//            String lvl3Name = "";
//            if (lvlList.size() > 0) {
//                lvl2Code = lvlList.get(0).getLvl2Code();
//                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//                assistantName = lvlList.get(0).getAssistantName();
//                lvl3Name = lvlList.get(0).getLvl3Name();
//            } else {
//                //架构错误
//            }
            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**地区经理提交 cuspost_quarter_apply_state_info*/
            String approver = "";//当前审批人
            String buttonEffect = "";//按钮是否有效

            if (UserConstant.POST_CODE1.equals(postCode) && UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ.equals(customerTypeCode)) {
                //地区经理查询自己的数据
                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    List<CuspostQuarterChainstoreHqAddDsm> existList = cuspostQuarterChainstoreHqAddDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterChainstoreHqAddDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    List<CuspostQuarterChainstoreHqChangeDsm> existList = cuspostQuarterChainstoreHqChangeDsmMapper.selectList(
                            new QueryWrapper<CuspostQuarterChainstoreHqChangeDsm>()
                                    .eq("manageYear", manageYear)
                                    .eq("manageQuarter", manageQuarter)
                                    .eq("lvl4Code", lvl4Code)
                    );
                    if (StringUtils.isEmpty(existList) || existList.size() < 1) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                    }
                }

                //按钮是否有效
                buttonEffect = "chainstoreHqButtonEffect";
                //获取审批人
//                approver = assistantName;
                approver = "大区助理";

                //更新申请编码状态 cuspost_quarter_apply_state_info
                CuspostQuarterApplyStateInfo cuspostApplyStateQuarterInfo = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper = new UpdateWrapper<>();
                updateWrapper.set("chainstoreHqApplyStateCode", UserConstant.APPLY_STATE_CODE_2);
                updateWrapper.eq("typeCode", typeCode);
                updateWrapper.eq("manageYear", manageYear);
                updateWrapper.eq("manageQuarter", manageQuarter);
                updateWrapper.eq("lvl4Code", lvl4Code);
                int insertCount = cuspostQuarterApplyStateInfoMapper.update(cuspostApplyStateQuarterInfo, updateWrapper);
                if (insertCount <= 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                }

                if (UserConstant.APPLY_TYPE_CODE1.equals(typeCode)) {//新增
                    //更新申请编码状态 cuspost_quarter_chainstore_hq_add_dsm
                    CuspostQuarterChainstoreHqAddDsm cuspostChainstoreHqApplyQuarterInfo = new CuspostQuarterChainstoreHqAddDsm();
                    UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterChainstoreHqAddDsmMapper.update(cuspostChainstoreHqApplyQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
                if (UserConstant.APPLY_TYPE_CODE2.equals(typeCode)) {//变更删除
                    //更新申请编码状态 cuspost_quarter_chainstore_hq_change_dsm
                    CuspostQuarterChainstoreHqChangeDsm cuspostChainstoreHqChangeDeletionQuarterInfo = new CuspostQuarterChainstoreHqChangeDsm();
                    UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> updateWrapper2 = new UpdateWrapper<>();
                    updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_2);
                    updateWrapper2.set("approver", approver);
                    updateWrapper2.eq("manageYear", manageYear);
                    updateWrapper2.eq("manageQuarter", manageQuarter);
                    updateWrapper2.eq("lvl4Code", lvl4Code);
                    int insertCount2 = cuspostQuarterChainstoreHqChangeDsmMapper.update(cuspostChainstoreHqChangeDeletionQuarterInfo, updateWrapper2);

                    if (insertCount2 <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "提交失败！");
                    }
                }
            }

            /**大区助理提交
             * cuspost_quarter_chainstore_hq_add_dsm
             * cuspost_quarter_chainstore_hq_add_assistant
             * cuspost_quarter_chainstore_hq_change_dsm
             * cuspost_quarter_chainstore_hq_change_assistant
             * cuspost_quarter_apply_state_info
             * cuspost_quarter_apply_state_region_info
             * */
            if (UserConstant.POST_CODE2.equals(postCode) && UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ.equals(customerTypeCode)) {
                //大区助理查询地区和大区的数据
                List<CuspostQuarterChainstoreHqAddDsm> existList1 = cuspostQuarterChainstoreHqAddDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterChainstoreHqAddDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterChainstoreHqAddAssistant> existList2 = cuspostQuarterChainstoreHqAddAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterChainstoreHqAddAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterChainstoreHqChangeDsm> existList3 = cuspostQuarterChainstoreHqChangeDsmMapper.selectList(
                        new QueryWrapper<CuspostQuarterChainstoreHqChangeDsm>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                List<CuspostQuarterChainstoreHqChangeAssistant> existList4 = cuspostQuarterChainstoreHqChangeAssistantMapper.selectList(
                        new QueryWrapper<CuspostQuarterChainstoreHqChangeAssistant>()
                                .eq("manageYear", manageYear)
                                .eq("manageQuarter", manageQuarter)
                                .eq("lvl2Code", lvl2Code)
                );
                if ((StringUtils.isEmpty(existList1) || existList1.size() < 1)
                        && (StringUtils.isEmpty(existList2) || existList2.size() < 1)
                        && (StringUtils.isEmpty(existList3) || existList3.size() < 1)
                        && (StringUtils.isEmpty(existList4) || existList4.size() < 1)) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "没有数据,请确认后再提交！");
                }

                //按钮是否有效
                buttonEffect = "chainstoreHqButtonEffect";
                //查询审批人：approver
//                approver = lvl3Name;
                approver = "大区总监";

                /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_dsm*/
                CuspostQuarterChainstoreHqAddDsm info1 = new CuspostQuarterChainstoreHqAddDsm();
                UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> updateWrapper1 = new UpdateWrapper<>();
                updateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper1.set("approver", approver);
                updateWrapper1.set("verifyRemark", assistantRemark);
                updateWrapper1.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper1.eq("manageYear", manageYear);
                updateWrapper1.eq("manageQuarter", manageQuarter);
                updateWrapper1.eq("lvl2Code", lvl2Code);
                int insertCount1 = cuspostQuarterChainstoreHqAddDsmMapper.update(info1, updateWrapper1);

                /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_assistant*/
                CuspostQuarterChainstoreHqAddAssistant info2 = new CuspostQuarterChainstoreHqAddAssistant();
                UpdateWrapper<CuspostQuarterChainstoreHqAddAssistant> updateWrapper2 = new UpdateWrapper<>();
                updateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper2.set("approver", approver);
                updateWrapper2.set("verifyRemark", assistantRemark);
                updateWrapper2.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper2.eq("manageYear", manageYear);
                updateWrapper2.eq("manageQuarter", manageQuarter);
                updateWrapper2.eq("lvl2Code", lvl2Code);
                int insertCount2 = cuspostQuarterChainstoreHqAddAssistantMapper.update(info2, updateWrapper2);

                /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_dsm*/
                CuspostQuarterChainstoreHqChangeDsm info3 = new CuspostQuarterChainstoreHqChangeDsm();
                UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> updateWrapper3 = new UpdateWrapper<>();
                updateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper3.set("approver", approver);
                updateWrapper3.set("verifyRemark", assistantRemark);
                updateWrapper3.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper3.eq("manageYear", manageYear);
                updateWrapper3.eq("manageQuarter", manageQuarter);
                updateWrapper3.eq("lvl2Code", lvl2Code);
                int insertCount3 = cuspostQuarterChainstoreHqChangeDsmMapper.update(info3, updateWrapper3);

                /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_assistant*/
                CuspostQuarterChainstoreHqChangeAssistant info4 = new CuspostQuarterChainstoreHqChangeAssistant();
                UpdateWrapper<CuspostQuarterChainstoreHqChangeAssistant> updateWrapper4 = new UpdateWrapper<>();
                updateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_4);
                updateWrapper4.set("approver", approver);
                updateWrapper4.set("verifyRemark", assistantRemark);
                updateWrapper4.set("approvalOpinion", ""); //20230525 大区助理提交时审批意见清空
                updateWrapper4.eq("manageYear", manageYear);
                updateWrapper4.eq("manageQuarter", manageQuarter);
                updateWrapper4.eq("lvl2Code", lvl2Code);
                int insertCount4 = cuspostQuarterChainstoreHqChangeAssistantMapper.update(info4, updateWrapper4);

                /**更新申请编码状态 cuspost_quarter_apply_state_info*/
                CuspostQuarterApplyStateInfo info5 = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper5 = new UpdateWrapper<>();
                updateWrapper5.set("chainstoreHqApplyStateCode", UserConstant.APPLY_STATE_CODE_4);
                //updateWrapper5.eq("typeCode", typeCode); 大区助理提交时，新增和变更删除都一起提交
                updateWrapper5.eq("manageYear", manageYear);
                updateWrapper5.eq("manageQuarter", manageQuarter);
                updateWrapper5.eq("lvl2Code", lvl2Code);
                int insertCount5 = cuspostQuarterApplyStateInfoMapper.update(info5, updateWrapper5);

                //20230613 没有数据的状态变为已完成 START
                customerPostMapper.updateQuarterApplyStateChainstoreHqAddAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateChainstoreHqAddDsmNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateChainstoreHqChangeAssNoData(manageYear, manageQuarter, lvl2Code);
                customerPostMapper.updateQuarterApplyStateChainstoreHqChangeDsmNoData(manageYear, manageQuarter, lvl2Code);
                //20230613 没有数据的状态变为已完成 END

                /**更新申请编码状态 cuspost_quarter_apply_state_region_info*/
                CuspostQuarterApplyStateRegionInfo insertModel = new CuspostQuarterApplyStateRegionInfo();
                insertModel.setManageYear(BigDecimal.valueOf(manageYear));
                insertModel.setManageQuarter(manageQuarter);
                insertModel.setCustomerTypeCode(customerTypeCode);
                insertModel.setRegion(lvl2Code);
                insertModel.setApplyStateCode(UserConstant.APPLY_STATE_CODE_4);
                insertModel.setPostCode(UserConstant.POST_CODE3);
                insertModel.setIsOver("0");
                if ("大于".equals(applyJudge)) {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A004");
                    } else {
                        insertModel.setApprovalProcessCode("A002");
                    }

                } else {
                    if ("3".equals(customerTypeCode)) { //商务
                        insertModel.setApprovalProcessCode("A003");
                    } else {
                        insertModel.setApprovalProcessCode("A001");
                    }
                }
                //20230613 如果没有数据不进入到总监审批 START
                if ((!StringUtils.isEmpty(existList1) && existList1.size() > 0)
                        || (!StringUtils.isEmpty(existList2) && existList2.size() > 0)) {
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE1);
                    int insertCountInsert1 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                if ((!StringUtils.isEmpty(existList3) && existList3.size() > 0)
                        || (!StringUtils.isEmpty(existList4) && existList4.size() > 0)) {
                    //变更删除的场合再创建一遍
                    insertModel.setTypeCode(UserConstant.APPLY_TYPE_CODE2);
                    int insertCountInsert2 = cuspostQuarterApplyStateRegionInfoMapper.insert(insertModel);
                }
                //20230613 如果没有数据不进入到总监审批 END
            }
            resultMap.put(buttonEffect, "0"); //按钮不可用

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }
    //endregion

    /*********************************************季度客岗调整变更删除****************************************************/
    //region 季度客岗调整变更删除

    /**
     * 查询季度医院变更删除数据
     */
    @ApiOperation(value = "查询季度医院变更删除数据", notes = "查询季度医院变更删除数据")
    @RequestMapping(value = "/queryHospitalChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryHospitalChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            //架构查询数据权限
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("hospitalCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryHospitalChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }
            List<Map<String, Object>> list = StringUtils.isEmpty(result) ? null : result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }


    /**
     * 查询季度零售终端变更删除数据
     */
    @ApiOperation(value = "查询季度零售终端变更删除数据", notes = "查询季度零售终端变更删除数据")
    @RequestMapping(value = "/queryRetailChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryRetailChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            //架构查询数据权限
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("retailCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryRetailChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }
            List<Map<String, Object>> list = StringUtils.isEmpty(result) ? null : result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 查询季度商务打单商变更删除数据
     */
    @ApiOperation(value = "查询季度商务打单商变更删除数据", notes = "查询季度商务打单商变更删除数据")
    @RequestMapping(value = "/queryDistributorChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryDistributorChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            //架构查询数据权限
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("distributorCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryDistributorChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }
            List<Map<String, Object>> list = StringUtils.isEmpty(result) ? null : result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 查询季度连锁总部变更删除数据
     */
    @ApiOperation(value = "查询季度连锁总部变更删除数据", notes = "查询季度连锁总部变更删除数据")
    @RequestMapping(value = "/queryChainstoreHqChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryChainstoreHqChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 检索处理
            //架构查询数据权限
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("chainstoreHqCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryChainstoreHqChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }
            List<Map<String, Object>> list = StringUtils.isEmpty(result) ? null : result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载季度医院变更删除数据
     */
    @ApiOperation(value = "下载季度医院变更删除数据", notes = "下载季度医院变更删除数据")
    @RequestMapping(value = "/exprotHospitalChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotHospitalChangeDeletionQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region");                                 // 大区
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("hospitalCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryHospitalChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_HOSPITAL_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * @MethodName 下载季度医院变更删除数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度医院变更删除数据 D&A下载按照上传模板顺序", notes = "下载季度医院变更删除数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotHospitalChangeDeletionQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotHospitalChangeDeletionQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region");                                 // 大区
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("hospitalCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryHospitalChangeDeletionQuarterInfo(page
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_HOSPITAL_CHANGE_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度医院变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载季度零售终端变更删除数据
     */
    @ApiOperation(value = "下载季度零售终端变更删除数据", notes = "下载季度零售终端变更删除数据")
    @RequestMapping(value = "/exprotRetailChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotRetailChangeDeletionQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("retailCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryRetailChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_RETAIL_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度零售终端变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * @MethodName 下载季度零售终端变更删除数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度零售终端变更删除数据 D&A下载按照上传模板顺序", notes = "下载季度零售终端变更删除数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotRetailChangeDeletionQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotRetailChangeDeletionQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("retailCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryRetailChangeDeletionQuarterInfo(page
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_RETAIL_CHANGE_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度零售终端变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载季度商务打单商变更删除数据
     */
    @ApiOperation(value = "下载季度商务打单商变更删除数据", notes = "下载季度商务打单商变更删除数据")
    @RequestMapping(value = "/exprotDistributorChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotDistributorChangeDeletionQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("distributorCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryDistributorChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DISTRIBUTOR_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度商务变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * @MethodName 下载季度商务打单商变更删除数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度商务打单商变更删除数据 D&A下载按照上传模板顺序", notes = "下载季度商务打单商变更删除数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotDistributorChangeDeletionQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotDistributorChangeDeletionQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region"); // postCode
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("distributorCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryDistributorChangeDeletionQuarterInfo(page
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_DISTRIBUTOR_CHANGE_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度商务变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载季度连锁总部变更删除数据
     */
    @ApiOperation(value = "下载季度连锁总部变更删除数据", notes = "下载季度连锁总部变更删除数据")
    @RequestMapping(value = "/exprotChainstoreHqChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotChainstoreHqChangeDeletionQuarterInfo(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
//            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); // 关键字 ？？？
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());
////            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
//            List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
////            String lvl2Code = "";
////            String lvl3Code = "";
//            String lvl4Code = "";
//            if (lvlList.size() > 0) {
////                lvl2Code = lvlList.get(0).getLvl2Code();
////                lvl3Code = lvlList.get(0).getLvl3Code();
//                lvl4Code = lvlList.get(0).getLvl4Code();
//            } else {
//                //架构错误
//            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("chainstoreHqCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryChainstoreHqChangeDeletionQuarterInfo(page
//                        , postCode, lvl2Code, lvl4Code
//                        , postCode, lvl2Codes, lvl4Code
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName //20230302 排序
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_CHAINSTORE_HQ_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度连锁变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }


    /**
     * @MethodName 下载季度连锁总部变更删除数据 D&A下载按照上传模板顺序
     * @Remark 20240222
     * @Authror Hazard
     * @Date 2024/2/23 10:56
     */
    @ApiOperation(value = "下载季度连锁总部变更删除数据 D&A下载按照上传模板顺序", notes = "下载季度连锁总部变更删除数据 D&A下载按照上传模板顺序")
    @RequestMapping(value = "/exprotChainstoreHqChangeDeletionQuarterInfoForDaUpload", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotChainstoreHqChangeDeletionQuarterInfoForDaUpload(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); // 客户编码
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String beforeDsmCode = object.getString("beforeDsmCode"); // 变更前DSM岗位编码
            String beforeRepCode = object.getString("beforeRepCode"); // 变更前rep岗位编码
            String dsmCode = object.getString("dsmCode"); // 变更后DSM岗位编码
            String repCode = object.getString("repCode"); // 变更后rep岗位编码
            String adjustTypeCode = object.getString("adjustTypeCode"); // 调整类型
            String postCode = object.getString("postCode"); // postCode
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String region = object.getString("region"); // region
            String orderName = object.getString("orderName"); // 20230302 排序

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            /**获取大区，地区等岗位编码*/
            String lvl4Code = customerPostMapper.queryLvl4Code(nowYM, loginUser.getUserCode());

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
                            .eq("chainstoreHqCheckBox", "1")
            );
            if (!StringUtils.isEmpty(cuspostInfo)) {
                result = customerPostMapper.queryChainstoreHqChangeDeletionQuarterInfo(page
                        , postCode, lvl4Code
                        , manageYear, manageQuarter, manageMonth, customerCode, customerName, province, city
                        , beforeDsmCode, beforeRepCode, dsmCode, repCode, adjustTypeCode
                        , region
                        , orderName
                );
            }

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_CHAINSTORE_HQ_CHANGE_EXPROT_FOR_UPLOAD);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度连锁变更删除数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 更新季度医院变更删除数据（删除/变更，修改变更内容都用这个）
     */
    @ApiOperation(value = "更新季度医院变更删除数据", notes = "更新季度医院变更删除数据")
    @RequestMapping(value = "/updateHospitalChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper updateHospitalChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String customerName = object.getString("customerName");                     // 客户名称
            String address = object.getString("address");                               // 地址
            String province = object.getString("province");                             // 省份
            String city = object.getString("city");                                     // 城市
            String adjustTypeCode = object.getString("adjustTypeCode");                 // 调整类型
            String applyContent = object.getString("applyContent");                     // 申诉内容
//            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
//            String repCode = object.getString("repCode");                               // REP岗位代码
            String dsmCode = StringUtils.isEmpty(object.getString("dsmCode")) ? "" : object.getString("dsmCode");// DSM岗位代码
            String repCode = StringUtils.isEmpty(object.getString("repCode")) ? "" : object.getString("repCode");// REP岗位代码
            String otherRemark = object.getString("otherRemark");                       // 其他备注
            String postCode = object.getString("postCode");                             // postCode
            String region = object.getString("region");                                 // region
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(address) || StringUtils.isEmpty(adjustTypeCode) || StringUtils.isEmpty(applyContent)
//                    || StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(repCode) //20240228 如果调整类型为2则不校验DSM编码，REP编码为空
                    || StringUtils.isEmpty(applyCode) // 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 S
            if (!"2".equals(adjustTypeCode) && (StringUtils.isEmpty(dsmCode) || StringUtils.isEmpty(repCode))) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "岗位代码不可以为空！");
            }
            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 E

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();


            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(commonUtils.getTodayYM2(), dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //获取repName,repCwid
            Map<String, String> repMap = customerPostMapper.getDataNameByDataCode(commonUtils.getTodayYM2(), repCode);
            String repName = null;
            String repCwid = null;
            if (!StringUtils.isEmpty(repMap)) {
                repName = repMap.get("userName");
                repCwid = repMap.get("cwid");
            } else {
                //架构错误
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());

            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_HOSPITAL);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_HOSPITAL);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }

            /**校验 架构城市关系*/
            if (!StringUtils.isEmpty(repCode)) { //20240228 如果调整类型为2则不校验DSM编码，REP编码为空
                int countFromStructureCity = customerPostMapper.queryCountFromStructureCity(nowYM, repCode, city);
                if (countFromStructureCity < 1) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "架构城市关系错误", "架构城市关系不正确！");
                }
            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterHospitalChangeDsm infoExist = cuspostQuarterHospitalChangeDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterHospitalChangeDsm>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterHospitalChangeDsm infoInsert = new CuspostQuarterHospitalChangeDsm();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setDsmCode(dsmCode);
                    infoInsert.setDsmCwid(dsmCwid);
                    infoInsert.setDsmName(dsmName);
                    infoInsert.setRepCode(repCode);
                    infoInsert.setRepCwid(repCwid);
                    infoInsert.setRepName(repName);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterHospitalChangeDsmMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterHospitalChangeDsm infoUpdate = new CuspostQuarterHospitalChangeDsm();
                    UpdateWrapper<CuspostQuarterHospitalChangeDsm> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("repCode", repCode);
                    updateWrapper.set("repCwid", repCwid);
                    updateWrapper.set("repName", repName);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterHospitalChangeDsmMapper.update(infoUpdate, updateWrapper);
                }

            } else if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterHospitalChangeAssistant infoExist = cuspostQuarterHospitalChangeAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterHospitalChangeAssistant>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterHospitalChangeAssistant infoInsert = new CuspostQuarterHospitalChangeAssistant();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setDsmCode(dsmCode);
                    infoInsert.setDsmCwid(dsmCwid);
                    infoInsert.setDsmName(dsmName);
                    infoInsert.setRepCode(repCode);
                    infoInsert.setRepCwid(repCwid);
                    infoInsert.setRepName(repName);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterHospitalChangeAssistantMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterHospitalChangeAssistant infoUpdate = new CuspostQuarterHospitalChangeAssistant();
                    UpdateWrapper<CuspostQuarterHospitalChangeAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("repCode", repCode);
                    updateWrapper.set("repCwid", repCwid);
                    updateWrapper.set("repName", repName);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterHospitalChangeAssistantMapper.update(infoUpdate, updateWrapper);
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 更新季度零售终端变更删除数据（删除/变更，修改变更内容都用这个）
     */
    @ApiOperation(value = "更新季度零售终端变更删除数据", notes = "新增季度零售终端变更删除数据")
    @RequestMapping(value = "/updateRetailChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper updateRetailChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String customerName = object.getString("customerName");                     // 客户名称
            String address = object.getString("address");                               // 地址
            String province = object.getString("province");                             // 省份
            String city = object.getString("city");                                     // 城市
            String adjustTypeCode = object.getString("adjustTypeCode");                 // 调整类型
            String applyContent = object.getString("applyContent");                     // 申诉内容
            String otherRemark = object.getString("otherRemark");                       // 其他备注
            String postCode = object.getString("postCode");                             // postCode
            String region = object.getString("region");                                 // region
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(address) || StringUtils.isEmpty(adjustTypeCode) || StringUtils.isEmpty(applyContent)
                    || StringUtils.isEmpty(applyCode) // 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

            //20230530 级别为白金或金的客户不能被删除
            if ("2".equals(adjustTypeCode)) {
                List<Map<String, String>> maps = customerPostMapper.queryCuspostQuarterRetailById(manageMonth, customerCode);
                if (!StringUtils.isEmpty(maps) && maps.size() > 0) {
                    if ("金".equals(maps.get(0).get("rt_segmentation")) || "白金".equals(maps.get(0).get("rt_segmentation"))) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "级别为白金或金的客户不能被删除！");
                    }
                }
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());

            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_HOSPITAL);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_HOSPITAL);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterRetailChangeDsm infoExist = cuspostQuarterRetailChangeDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterRetailChangeDsm>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterRetailChangeDsm infoInsert = new CuspostQuarterRetailChangeDsm();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterRetailChangeDsmMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterRetailChangeDsm infoUpdate = new CuspostQuarterRetailChangeDsm();
                    UpdateWrapper<CuspostQuarterRetailChangeDsm> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterRetailChangeDsmMapper.update(infoUpdate, updateWrapper);
                }

            } else if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterRetailChangeAssistant infoExist = cuspostQuarterRetailChangeAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterRetailChangeAssistant>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterRetailChangeAssistant infoInsert = new CuspostQuarterRetailChangeAssistant();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterRetailChangeAssistantMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterRetailChangeAssistant infoUpdate = new CuspostQuarterRetailChangeAssistant();
                    UpdateWrapper<CuspostQuarterRetailChangeAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterRetailChangeAssistantMapper.update(infoUpdate, updateWrapper);
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 更新季度商务打单商变更删除数据（删除/变更，修改变更内容都用这个）
     */
    @ApiOperation(value = "更新季度商务打单商变更删除数据", notes = "更新季度商务打单商变更删除数据")
    @RequestMapping(value = "/updateDistributorChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper updateDistributorChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String customerName = object.getString("customerName");                     // 客户名称
            String address = object.getString("address");                               // 地址
            String province = object.getString("province");                             // 省市
            String city = object.getString("city");                                     // 城市
            String adjustTypeCode = object.getString("adjustTypeCode");                 // 调整类型
            String applyContent = object.getString("applyContent");                     // 申诉内容
//            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String dsmCode = StringUtils.isEmpty(object.getString("dsmCode")) ? "" : object.getString("dsmCode");                               // DSM岗位代码
            String otherRemark = object.getString("otherRemark");                       // 其他备注
            String postCode = object.getString("postCode");                             // postCode
            String region = object.getString("region");                                 // region
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(address) || StringUtils.isEmpty(adjustTypeCode)
                    || StringUtils.isEmpty(applyContent)
//                    || StringUtils.isEmpty(dsmCode) //20240228 如果调整类型为2则不校验DSM编码，REP编码为空
                    || StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 S
            if (!"2".equals(adjustTypeCode) && (StringUtils.isEmpty(dsmCode))) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "岗位代码不可以为空！");
            }
            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 E

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(commonUtils.getTodayYM2(), dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterDistributorChangeDsm infoExist = cuspostQuarterDistributorChangeDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterDistributorChangeDsm>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterDistributorChangeDsm infoInsert = new CuspostQuarterDistributorChangeDsm();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setDsmCode(dsmCode);
                    infoInsert.setDsmCwid(dsmCwid);
                    infoInsert.setDsmName(dsmName);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterDistributorChangeDsmMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterDistributorChangeDsm infoUpdate = new CuspostQuarterDistributorChangeDsm();
                    UpdateWrapper<CuspostQuarterDistributorChangeDsm> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterDistributorChangeDsmMapper.update(infoUpdate, updateWrapper);
                }

            } else if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterDistributorChangeAssistant infoExist = cuspostQuarterDistributorChangeAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterDistributorChangeAssistant>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterDistributorChangeAssistant infoInsert = new CuspostQuarterDistributorChangeAssistant();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setDsmCode(dsmCode);
                    infoInsert.setDsmCwid(dsmCwid);
                    infoInsert.setDsmName(dsmName);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterDistributorChangeAssistantMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterDistributorChangeAssistant infoUpdate = new CuspostQuarterDistributorChangeAssistant();
                    UpdateWrapper<CuspostQuarterDistributorChangeAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterDistributorChangeAssistantMapper.update(infoUpdate, updateWrapper);
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 更新季度连锁总部变更删除数据（删除/变更，修改变更内容都用这个）
     */
    @ApiOperation(value = "更新季度连锁总部变更删除数据", notes = "更新季度连锁总部变更删除数据")
    @RequestMapping(value = "/updateChainstoreHqChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper updateChainstoreHqChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String customerName = object.getString("customerName");                     // 客户名称
            String address = object.getString("address");                               // 地址
            String province = object.getString("province");                             // 省份
            String city = object.getString("city");                                     // 城市
            String adjustTypeCode = object.getString("adjustTypeCode");                 // 调整类型
            String applyContent = object.getString("applyContent");                     // 申诉内容
//            String dsmCode = object.getString("dsmCode");                               // DSM岗位代码
            String dsmCode = StringUtils.isEmpty(object.getString("dsmCode")) ? "" : object.getString("dsmCode");                               // DSM岗位代码
            String otherRemark = object.getString("otherRemark");                       // 其他备注
            String postCode = object.getString("postCode");                             // postCode
            String region = object.getString("region");                                 // region
            String kaUpStreamLeChoId = object.getString("kaUpStreamLeChoId");           // 归属上级编码 20230301
            String kaUpStreamLeName = object.getString("kaUpStreamLeName");             // 归属上级名称 20230301
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode) || StringUtils.isEmpty(customerName)
                    || StringUtils.isEmpty(address) || StringUtils.isEmpty(adjustTypeCode)
//                    || StringUtils.isEmpty(dsmCode) //20240228 如果调整类型为2则不校验DSM编码，REP编码为空
                    || StringUtils.isEmpty(applyContent)
                    || StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 S
            if (!"2".equals(adjustTypeCode) && (StringUtils.isEmpty(dsmCode))) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "岗位代码不可以为空！");
            }
            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 E

            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 S
            if (( StringUtils.isEmpty(dsmCode)) && !"2".equals(adjustTypeCode)
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "DSM岗位代码不可以为空！");
            }
            //20240228 如果调整类型为2则不校验DSM编码，REP编码为空 E

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(commonUtils.getTodayYM2(), dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Code, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ);
//            int countFromRegionToCity = customerPostMapper.queryCountFromRegionToCity(nowYM, province, city, lvl2Codes, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ);
            if (countFromRegionToCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "业务覆盖城市不正确！");
            }

            /**获取大区*/
//            String region = customerPostMapper.queryRegionFromRegionToCity(nowYM, province, city, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR);
//            if (StringUtils.isEmpty(region)) {
//                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "业务覆盖城市错误", "没有对应大区信息！");
//            }

            if (UserConstant.POST_CODE1.equals(postCode)) {
                //获取既存数据
                CuspostQuarterChainstoreHqChangeDsm infoExist = cuspostQuarterChainstoreHqChangeDsmMapper.selectOne(
                        new QueryWrapper<CuspostQuarterChainstoreHqChangeDsm>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterChainstoreHqChangeDsm infoInsert = new CuspostQuarterChainstoreHqChangeDsm();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setDsmCode(dsmCode);
                    infoInsert.setDsmCwid(dsmCwid);
                    infoInsert.setDsmName(dsmName);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setKaUpStreamLeChoId(kaUpStreamLeChoId); //20230301
                    infoInsert.setKaUpStreamLeName(kaUpStreamLeName); //20230301
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterChainstoreHqChangeDsmMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterChainstoreHqChangeDsm infoUpdate = new CuspostQuarterChainstoreHqChangeDsm();
                    UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
                    updateWrapper.set("kaUpStreamLeChoId", kaUpStreamLeChoId); //20230301
                    updateWrapper.set("kaUpStreamLeName", kaUpStreamLeName); //20230301
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterChainstoreHqChangeDsmMapper.update(infoUpdate, updateWrapper);
                }

            } else if (UserConstant.POST_CODE2.equals(postCode)) {
                //获取既存数据
                CuspostQuarterChainstoreHqChangeAssistant infoExist = cuspostQuarterChainstoreHqChangeAssistantMapper.selectOne(
                        new QueryWrapper<CuspostQuarterChainstoreHqChangeAssistant>()
//                                .eq("manageYear", manageYear)
//                                .eq("manageQuarter", manageQuarter)
//                                .eq("customerCode", customerCode)
                                .eq("applyCode", applyCode)// 20230424 变更申请编码
                );

                if (StringUtils.isEmpty(infoExist)) { //新增
                    CuspostQuarterChainstoreHqChangeAssistant infoInsert = new CuspostQuarterChainstoreHqChangeAssistant();
                    infoInsert.setApplyCode(applyCode);// 20230424 变更申请编码
                    infoInsert.setManageYear(BigDecimal.valueOf(manageYear));
                    infoInsert.setManageQuarter(manageQuarter);
                    infoInsert.setYearMonth(BigDecimal.valueOf(manageMonth));
                    infoInsert.setCustomerCode(customerCode);
                    infoInsert.setCustomerName(customerName);
                    infoInsert.setAddress(address);
                    infoInsert.setAdjustTypeCode(adjustTypeCode);
                    infoInsert.setApplyContent(applyContent);
                    infoInsert.setOtherRemark(otherRemark);
                    infoInsert.setDsmCode(dsmCode);
                    infoInsert.setDsmCwid(dsmCwid);
                    infoInsert.setDsmName(dsmName);
                    infoInsert.setPostCode(postCode);
                    infoInsert.setLvl2Code(lvl2Code);
//                    infoInsert.setLvl2Code(region);
//                    infoInsert.setLvl3Code(lvl3Code);
                    infoInsert.setLvl3Code(null);
                    infoInsert.setLvl4Code(lvl4Code);
                    infoInsert.setApplyStateCode(UserConstant.APPLY_STATE_CODE_1);//20230524
                    infoInsert.setKaUpStreamLeChoId(kaUpStreamLeChoId); //20230301
                    infoInsert.setKaUpStreamLeName(kaUpStreamLeName); //20230301
                    //20230625 START
                    infoInsert.setInsertUser(loginUser.getUserCode());
                    infoInsert.setInsertTime(new Date());
                    //20230625 END

                    int insertCount = cuspostQuarterChainstoreHqChangeAssistantMapper.insert(infoInsert);
                    if (insertCount <= 0) {
                        return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据新增失败！");
                    }
                } else {
                    //更新既存数据
                    CuspostQuarterChainstoreHqChangeAssistant infoUpdate = new CuspostQuarterChainstoreHqChangeAssistant();
                    UpdateWrapper<CuspostQuarterChainstoreHqChangeAssistant> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.set("customerName", customerName);
                    updateWrapper.set("address", address);
                    updateWrapper.set("adjustTypeCode", adjustTypeCode);
                    updateWrapper.set("applyContent", applyContent);
                    updateWrapper.set("dsmCode", dsmCode);
                    updateWrapper.set("dsmCwid", dsmCwid);
                    updateWrapper.set("dsmName", dsmName);
                    updateWrapper.set("otherRemark", otherRemark);
                    updateWrapper.set("updateUser", loginUser.getUserCode());
                    updateWrapper.set("updateTime", new Date());
                    updateWrapper.set("kaUpStreamLeChoId", kaUpStreamLeChoId); //20230301
                    updateWrapper.set("kaUpStreamLeName", kaUpStreamLeName); //20230301
//                    updateWrapper.eq("manageYear", BigDecimal.valueOf(manageYear));
//                    updateWrapper.eq("manageQuarter", manageQuarter);
//                    updateWrapper.eq("customerCode", customerCode);
                    updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
                    cuspostQuarterChainstoreHqChangeAssistantMapper.update(infoUpdate, updateWrapper);
                }
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 重置季度医院变更删除数据
     */
    @ApiOperation(value = "重置季度医院变更删除数据", notes = "重置季度医院变更删除数据")
    @RequestMapping(value = "/resetHospitalChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper resetHospitalChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode)
                    || StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterHospitalChangeDsm> updateWrapper = new UpdateWrapper<>();
//            updateWrapper.eq("manageYear", manageYear);
//            updateWrapper.eq("manageQuarter", manageQuarter);
//            updateWrapper.eq("customerCode", customerCode);
            updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount = cuspostQuarterHospitalChangeDsmMapper.delete(updateWrapper);

            UpdateWrapper<CuspostQuarterHospitalChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
//            updateWrapper2.eq("manageYear", manageYear);
//            updateWrapper2.eq("manageQuarter", manageQuarter);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterHospitalChangeAssistantMapper.delete(updateWrapper2);

            if (insertCount <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据重置失败！");
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 重置季度零售终端变更删除数据
     */
    @ApiOperation(value = "重置季度零售终端变更删除数据", notes = "重置季度零售终端变更删除数据")
    @RequestMapping(value = "/resetRetailChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper resetRetailChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode)
                    || StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterRetailChangeDsm> updateWrapper = new UpdateWrapper<>();
//            updateWrapper.eq("manageYear", manageYear);
//            updateWrapper.eq("manageQuarter", manageQuarter);
//            updateWrapper.eq("customerCode", customerCode);
            updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount = cuspostQuarterRetailChangeDsmMapper.delete(updateWrapper);

            UpdateWrapper<CuspostQuarterRetailChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
//            updateWrapper2.eq("manageYear", manageYear);
//            updateWrapper2.eq("manageQuarter", manageQuarter);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterRetailChangeAssistantMapper.delete(updateWrapper2);

            if (insertCount <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据重置失败！");
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 重置季度商务打单商变更删除数据
     */
    @ApiOperation(value = "重置季度商务打单商变更删除数据", notes = "重置季度商务打单商变更删除数据")
    @RequestMapping(value = "/resetDistributorChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper resetDistributorChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode)
                    || StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterDistributorChangeDsm> updateWrapper = new UpdateWrapper<>();
//            updateWrapper.eq("manageYear", manageYear);
//            updateWrapper.eq("manageQuarter", manageQuarter);
//            updateWrapper.eq("customerCode", customerCode);
            updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount = cuspostQuarterDistributorChangeDsmMapper.delete(updateWrapper);

            UpdateWrapper<CuspostQuarterDistributorChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
//            updateWrapper2.eq("manageYear", manageYear);
//            updateWrapper2.eq("manageQuarter", manageQuarter);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterDistributorChangeAssistantMapper.delete(updateWrapper2);

            if (insertCount <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据重置失败！");
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 重置季度连锁总部变更删除数据
     */
    @ApiOperation(value = "重置季度连锁总部变更删除数据", notes = "重置季度连锁总部变更删除数据")
    @RequestMapping(value = "/resetChainstoreHqChangeDeletionQuarterInfo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper resetChainstoreHqChangeDeletionQuarterInfo(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String customerCode = object.getString("customerCode");                     // 客户编码
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter) || StringUtils.isEmpty(customerCode)
                    || StringUtils.isEmpty(customerCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> updateWrapper = new UpdateWrapper<>();
//            updateWrapper.eq("manageYear", manageYear);
//            updateWrapper.eq("manageQuarter", manageQuarter);
//            updateWrapper.eq("customerCode", customerCode);
            updateWrapper.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount = cuspostQuarterChainstoreHqChangeDsmMapper.delete(updateWrapper);

            UpdateWrapper<CuspostQuarterChainstoreHqChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
//            updateWrapper2.eq("manageYear", manageYear);
//            updateWrapper2.eq("manageQuarter", manageQuarter);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterChainstoreHqChangeAssistantMapper.delete(updateWrapper2);

            if (insertCount <= 0 && insertCount2 <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据重置失败！");
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 上传季度医院变更删除数据
     */
    @ApiOperation(value = "上传季度医院变更删除数据", notes = "上传季度医院变更删除数据")
    @RequestMapping("/batchAddHospitalChangeDeletionQuarterInfo")
    @Transactional
    public Wrapper batchAddHospitalChangeDeletionQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
//            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_HOSPITAL_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_HOSPITAL_CHANGE_WITH_DATA);//20230220
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_hospital_change_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_hospital_change_assistant";
                }
                String errorFileName = hospitalChangeDeletionQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String hospitalChangeDeletionQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE2);
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE2);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }

            /**校验 架构城市关系*/
            String relation2 = customerPostMapper.updateCheckFromStructureCity(
                    tableEnNameTem, nowYM, manageMonth, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE2, null);
            if (relation2 != null) {
                String messageContent = " 客户 【" + relation2 + "】的架构城市关系不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //20230117 删除调整类型为空的数据（withData上传要用）
                customerPostMapper.deleteUploadChangeQuarterAdjustIsNull(tableEnName, fileId);
                //更新年度，季度,年月
                customerPostMapper.uploadHospitalChangeDeletionQuarterInfoYearQuarter(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth, nowYM
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);
                // 更新上传数据 页面可以修改
                // 更新新数据
                customerPostMapper.uploadHospitalChangeDeletionQuarterInfoUpdate(tableEnName, fileId, userCode);
                // 插入新数据
                customerPostMapper.uploadHospitalChangeDeletionQuarterInfoInsert(tableEnName, fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传季度零售终端变更删除数据
     */
    @ApiOperation(value = "上传季度零售终端变更删除数据", notes = "上传季度零售终端变更删除数据")
    @RequestMapping("/batchAddRetailChangeDeletionQuarterInfo")
    @Transactional
    public Wrapper batchAddRetailChangeDeletionQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
//            ？
//            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_RETAIL_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_RETAIL_CHANGE_WITH_DATA); //20230117 修改上传逻辑
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_retail_change_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_retail_change_assistant";
                }
                String errorFileName = retailChangeDeletionQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String retailChangeDeletionQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //20230530 级别为白金或金的客户不能被删除
            String relation = customerPostMapper.querySegmentationJinFromCuspostQuarterRetail(
                    tableEnNameTem, manageMonth, fileId);
            if (relation != null) {
                String messageContent = " 客户 【" + relation + "】的级别为白金或金的客户不能被删除，请确认！";
                errorMessageList.add(messageContent);
            }

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_RETAIL, fileId, UserConstant.APPLY_TYPE_CODE2);
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_RETAIL, fileId, UserConstant.APPLY_TYPE_CODE2);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //20230117 删除调整类型为空的数据（withData上传要用）
//                customerPostMapper.deleteUploadRetailChangeDeletionQuarterInfo(tableEnName,fileId);
                customerPostMapper.deleteUploadChangeQuarterAdjustIsNull(tableEnName, fileId);

                customerPostMapper.uploadRetailChangeDeletionQuarterInfoYearQuarter(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);

                // 更新上传数据 页面可以修改
                // 更新新数据
                customerPostMapper.uploadRetailChangeDeletionQuarterInfoUpdate(tableEnName, fileId, userCode);
                // 插入新数据
                customerPostMapper.uploadRetailChangeDeletionQuarterInfoInsert(tableEnName, fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传季度商务打单商变更删除数据
     */
    @ApiOperation(value = "上传季度商务打单商变更删除数据", notes = "上传季度商务打单商变更删除数据")
    @RequestMapping("/batchAddDistributorChangeDeletionQuarterInfo")
    @Transactional
    public Wrapper batchAddDistributorChangeDeletionQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
//            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DISTRIBUTOR_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DISTRIBUTOR_CHANGE_WITH_DATA);//20230220
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_distributor_change_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_distributor_change_assistant";
                }
                String errorFileName = distributorChangeDeletionQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String distributorChangeDeletionQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);

            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR, fileId, UserConstant.APPLY_TYPE_CODE2);
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_DISTRIBUTOR, fileId, UserConstant.APPLY_TYPE_CODE2);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //20230117 删除调整类型为空的数据（withData上传要用）
//                customerPostMapper.deleteUploadRetailChangeDeletionQuarterInfo(tableEnName,fileId);
                customerPostMapper.deleteUploadChangeQuarterAdjustIsNull(tableEnName, fileId);

                customerPostMapper.uploadDistributorChangeDeletionQuarterInfoYearQuarter(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth, nowYM
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);

                // 更新上传数据 页面可以修改
                // 更新新数据
                customerPostMapper.uploadDistributorChangeDeletionQuarterInfoUpdate(tableEnName, fileId, userCode);
                // 插入新数据
                customerPostMapper.uploadDistributorChangeDeletionQuarterInfoInsert(tableEnName, fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传季度连锁总部变更删除数据
     */
    @ApiOperation(value = "上传季度连锁总部变更删除数据", notes = "上传季度连锁总部变更删除数据")
    @RequestMapping("/batchAddChainstoreHqChangeDeletionQuarterInfo")
    @Transactional
    public Wrapper batchAddChainstoreHqChangeDeletionQuarterInfo(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");
            String postCode = request.getParameter("postCode");
            String region = request.getParameter("region");

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
//            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_CHAINSTORE_HQ_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_CHAINSTORE_HQ_CHANGE_WITH_DATA);//20230220
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String tableEnName = "";
                if (UserConstant.POST_CODE1.equals(postCode)) {
                    tableEnName = "cuspost_quarter_chainstore_hq_change_dsm";
                }
                if (UserConstant.POST_CODE2.equals(postCode)) {
                    tableEnName = "cuspost_quarter_chainstore_hq_change_assistant";
                }
                String errorFileName = chainstoreHqChangeDeletionQuarterInfoBatch(postCode, region, tableEnName, uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String chainstoreHqChangeDeletionQuarterInfoBatch(String postCode, String region, String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表，check省市，
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            /**获取大区，地区等岗位编码*/
//            List<CustomerPostModel> lvlList = getLvlCode(nowYM, postCode, loginUser.getUserCode());
            String lvl2Code = "";
            String lvl3Code = "";
            String lvl4Code = "";
            if (UserConstant.POST_CODE1.equals(postCode)) {
                List<CustomerPostModel> lvlList = customerPostMapper.queryDsmLevelCode(nowYM, loginUser.getUserCode());
                if (lvlList.size() > 0) {
                    lvl2Code = lvlList.get(0).getLvl2Code();
                    lvl3Code = lvlList.get(0).getLvl3Code();
                    lvl4Code = lvlList.get(0).getLvl4Code();
                } else {
                    //架构错误
                }
            } else {
                lvl2Code = region;
            }

            /**数据权限：获取大区助理大区经理商务总监*/
//            List<String> lvl2Codes = cuspostCommonService.getLvl2Codes(loginUser);


            /**校验 业务覆盖城市*/
            String relation1 = customerPostMapper.updateCheckFromRegionToCity(
                    tableEnNameTem, nowYM, manageMonth, lvl2Code, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ, fileId, UserConstant.APPLY_TYPE_CODE2);
//                    tableEnNameTem, nowYM, manageMonth, lvl2Codes, UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ, fileId, UserConstant.APPLY_TYPE_CODE2);
            if (relation1 != null) {
                String messageContent = " 客户 【" + relation1 + "】的业务覆盖城市不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //20230117 删除调整类型为空的数据（withData上传要用）
//                customerPostMapper.deleteUploadRetailChangeDeletionQuarterInfo(tableEnName,fileId);
                customerPostMapper.deleteUploadChangeQuarterAdjustIsNull(tableEnName, fileId);

                customerPostMapper.uploadChainstoreHqChangeDeletionQuarterInfoYearQuarter(tableEnName,
                        fileId, manageYear, manageQuarter, manageMonth, nowYM
                        , postCode, lvl2Code, lvl3Code, lvl4Code);
//                        , postCode, lvl3Code, lvl4Code);

                // 更新上传数据 页面可以修改
                // 更新新数据
                customerPostMapper.uploadChainstoreHqChangeDeletionQuarterInfoUpdate(tableEnName, fileId, userCode);
                // 插入新数据
                customerPostMapper.uploadChainstoreHqChangeDeletionQuarterInfoInsert(tableEnName, fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }
    //endregion


    /********************************************查询客岗上传数据记录信息***************************************************/
    //region 查询客岗上传数据记录信息

    /**
     * 查询客岗上传数据记录信息
     *
     * @param json
     * @return
     */
    @ApiOperation(value = "查询客岗上传数据记录信息", notes = "查询客岗上传数据记录信息")
    @RequestMapping(value = "/queryCuspostUploadRecordInfos", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryCuspostUploadRecordInfos(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String insertDateStart = object.getString("insertDateStart");           // 上传开始日
            String insertDateEnd = object.getString("insertDateEnd");               // 上传结束日
            String dealStateCode = object.getString("dealStateCode");               // 变更状态
            String flowYearMonth = object.getString("flowYearMonth");               // 流向年月
            String fileName = object.getString("fileName");                         // 文件名
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows");                          // 每页显示数据量
            Integer nextPage = object.getInteger("page");                          // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();

            String UserCode = null;
            // 管理员权限的场合产看所有记录
//            if (!UserConstant.ROLE_ADMIN.equals(currentLoginUser.getRoleCode())) {
//                UserCode = userInfo.getUserCode();
//            }
            UserCode = loginUser.getUserCode();
            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> groupInfos = customerPostMapper.queryCuspostUploadRecordInfos(page, insertDateStart,
                    insertDateEnd, dealStateCode, UserCode, fileName
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = groupInfos.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", groupInfos.getPages());
                resultMap.put("currPage", groupInfos.getCurrent());
                resultMap.put("totalCount", groupInfos.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载客岗导入文件
     *
     * @param json
     * @return
     */
    @ApiOperation(value = "下载客岗导入文件", notes = "下载导入文件")
    @RequestMapping(value = "/exportCuspostUploadFile", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exportCuspostUploadFile(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String keyId = object.getString("keyId");           // 主键

            CuspostQuarterDataUploadInfo ddiDataUploadInfo = cuspostQuarterDataUploadInfoMapper.selectById(keyId);

            if (ddiDataUploadInfo != null) {
                String newfileName = ddiDataUploadInfo.getNewFileName();
                String oldfileName = ddiDataUploadInfo.getUploadFileName();
                // 下载文件
                commonUtils.downloadFileWithDelete(request, oldfileName, cusPostFileUploadPath + newfileName, response);
            }

        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载客岗错误详细信息
     *
     * @param json
     * @return
     */
    @ApiOperation(value = "下载客岗错误详细信息", notes = "下载客岗错误详细信息")
    @RequestMapping(value = "/exportCuspostUploadErrorList", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exportCuspostUploadErrorList(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String keyId = object.getString("keyId");           // 主键
            CuspostQuarterDataUploadInfo ddiDataUploadInfo = cuspostQuarterDataUploadInfoMapper.selectById(keyId);
            if (ddiDataUploadInfo != null) {
                String errorFileName = ddiDataUploadInfo.getErrorFileName();
                // 下载文件
                commonUtils.downloadFileWithDelete(request, errorFileName, cusPostErrorfilePath + errorFileName, response);
            }

        } catch (Exception e) {
            logger.error(e);
        }
    }

    //endregion

    //region 大区总监审批等

    /**
     * 全部驳回
     */
    @ApiOperation(value = "全部驳回", notes = "全部驳回")
    @RequestMapping(value = "/rejectAll", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper rejectAll(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String region = object.getString("region");                                 // 大区
            String typeCode = object.getString("typeCode");                             // 新增，修改删除
            String customerTypeCode = object.getString("customerTypeCode");             // 1医院，2零售，3商务，4连锁
            String rejectPostCode = object.getString("rejectPostCode");                 // 驳回岗位 1地区经理，2大区助理

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter)
                    || StringUtils.isEmpty(region) || StringUtils.isEmpty(typeCode) || StringUtils.isEmpty(customerTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String applyStateCode = null;
            if (UserConstant.POST_CODE2.equals(rejectPostCode)) {
                //大区助理
                applyStateCode = UserConstant.APPLY_STATE_CODE_2;
            } else {
                //地区经理
                applyStateCode = UserConstant.APPLY_STATE_CODE_1;
            }

            /**全部驳回 变更内容
             * cuspost_quarter_xxx_add_dsm（申请状态，审批人）
             * cuspost_quarter_xxx_add_assistant（申请状态，审批人）
             * cuspost_quarter_xxx_change_dsm（申请状态，审批人）
             * cuspost_quarter_xxx_change_assistant（申请状态，审批人）
             * cuspost_quarter_apply_state_info（申请状态,包含新增和变更删除）
             * cuspost_quarter_apply_state_region_info（删除,包含新增和变更删除）
             * */
            int insertCount1 = 0;//insertCount
            int insertCount2 = 0;//insertCount
            int insertCount3 = 0;//insertCount
            int insertCount4 = 0;//insertCount
            String buttonEffect = "";//按钮是否有效
            String applyStateColumnName = "";//按钮是否有效
            switch (customerTypeCode) {
                case UserConstant.CUSTOMER_TYPE_HOSPITAL:
                    buttonEffect = "hospitalButtonEffect";
                    applyStateColumnName = "hospitalApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_hospital_add_dsm*/
                    CuspostQuarterHospitalAddDsm hospitalInfo1 = new CuspostQuarterHospitalAddDsm();
                    UpdateWrapper<CuspostQuarterHospitalAddDsm> hospitalUpdateWrapper1 = new UpdateWrapper<>();
                    hospitalUpdateWrapper1.set("applyStateCode", applyStateCode);
                    hospitalUpdateWrapper1.set("approver", null);
                    hospitalUpdateWrapper1.set("approvalOpinion", null);
                    hospitalUpdateWrapper1.set("verifyRemark", null);
                    hospitalUpdateWrapper1.eq("manageYear", manageYear);
                    hospitalUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterHospitalAddDsmMapper.update(hospitalInfo1, hospitalUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_hospital_add_assistant*/
                    CuspostQuarterHospitalAddAssistant hospitalInfo2 = new CuspostQuarterHospitalAddAssistant();
                    UpdateWrapper<CuspostQuarterHospitalAddAssistant> hospitalUpdateWrapper2 = new UpdateWrapper<>();
                    hospitalUpdateWrapper2.set("applyStateCode", applyStateCode);
                    hospitalUpdateWrapper2.set("approver", null);
                    hospitalUpdateWrapper2.set("approvalOpinion", null);
                    hospitalUpdateWrapper2.set("verifyRemark", null);
                    hospitalUpdateWrapper2.eq("manageYear", manageYear);
                    hospitalUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterHospitalAddAssistantMapper.update(hospitalInfo2, hospitalUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_hospital_change_dsm*/
                    CuspostQuarterHospitalChangeDsm hospitalInfo3 = new CuspostQuarterHospitalChangeDsm();
                    UpdateWrapper<CuspostQuarterHospitalChangeDsm> hospitalUpdateWrapper3 = new UpdateWrapper<>();
                    hospitalUpdateWrapper3.set("applyStateCode", applyStateCode);
                    hospitalUpdateWrapper3.set("approver", null);
                    hospitalUpdateWrapper3.set("approvalOpinion", null);
                    hospitalUpdateWrapper3.set("verifyRemark", null);
                    hospitalUpdateWrapper3.eq("manageYear", manageYear);
                    hospitalUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterHospitalChangeDsmMapper.update(hospitalInfo3, hospitalUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_hospital_change_assistant*/
                    CuspostQuarterHospitalChangeAssistant hospitalInfo4 = new CuspostQuarterHospitalChangeAssistant();
                    UpdateWrapper<CuspostQuarterHospitalChangeAssistant> hospitalUpdateWrapper4 = new UpdateWrapper<>();
                    hospitalUpdateWrapper4.set("applyStateCode", applyStateCode);
                    hospitalUpdateWrapper4.set("approver", null);
                    hospitalUpdateWrapper4.set("approvalOpinion", null);
                    hospitalUpdateWrapper4.set("verifyRemark", null);
                    hospitalUpdateWrapper4.eq("manageYear", manageYear);
                    hospitalUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterHospitalChangeAssistantMapper.update(hospitalInfo4, hospitalUpdateWrapper4);

                    break;
                case UserConstant.CUSTOMER_TYPE_RETAIL:
                    buttonEffect = "retailButtonEffect";
                    applyStateColumnName = "retailApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_retail_add_dsm*/
                    CuspostQuarterRetailAddDsm retailInfo1 = new CuspostQuarterRetailAddDsm();
                    UpdateWrapper<CuspostQuarterRetailAddDsm> retailUpdateWrapper1 = new UpdateWrapper<>();
                    retailUpdateWrapper1.set("applyStateCode", applyStateCode);
                    retailUpdateWrapper1.set("approver", null);
                    retailUpdateWrapper1.set("approvalOpinion", null);
                    retailUpdateWrapper1.set("verifyRemark", null);
                    retailUpdateWrapper1.eq("manageYear", manageYear);
                    retailUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterRetailAddDsmMapper.update(retailInfo1, retailUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_retail_add_assistant*/
                    CuspostQuarterRetailAddAssistant retailInfo2 = new CuspostQuarterRetailAddAssistant();
                    UpdateWrapper<CuspostQuarterRetailAddAssistant> retailUpdateWrapper2 = new UpdateWrapper<>();
                    retailUpdateWrapper2.set("applyStateCode", applyStateCode);
                    retailUpdateWrapper2.set("approver", null);
                    retailUpdateWrapper2.set("approvalOpinion", null);
                    retailUpdateWrapper2.set("verifyRemark", null);
                    retailUpdateWrapper2.eq("manageYear", manageYear);
                    retailUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterRetailAddAssistantMapper.update(retailInfo2, retailUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_retail_change_dsm*/
                    CuspostQuarterRetailChangeDsm retailInfo3 = new CuspostQuarterRetailChangeDsm();
                    UpdateWrapper<CuspostQuarterRetailChangeDsm> retailUpdateWrapper3 = new UpdateWrapper<>();
                    retailUpdateWrapper3.set("applyStateCode", applyStateCode);
                    retailUpdateWrapper3.set("approver", null);
                    retailUpdateWrapper3.set("approvalOpinion", null);
                    retailUpdateWrapper3.set("verifyRemark", null);
                    retailUpdateWrapper3.eq("manageYear", manageYear);
                    retailUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterRetailChangeDsmMapper.update(retailInfo3, retailUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_retail_change_assistant*/
                    CuspostQuarterRetailChangeAssistant retailInfo4 = new CuspostQuarterRetailChangeAssistant();
                    UpdateWrapper<CuspostQuarterRetailChangeAssistant> retailUpdateWrapper4 = new UpdateWrapper<>();
                    retailUpdateWrapper4.set("applyStateCode", applyStateCode);
                    retailUpdateWrapper4.set("approver", null);
                    retailUpdateWrapper4.set("approvalOpinion", null);
                    retailUpdateWrapper4.set("verifyRemark", null);
                    retailUpdateWrapper4.eq("manageYear", manageYear);
                    retailUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterRetailChangeAssistantMapper.update(retailInfo4, retailUpdateWrapper4);

                    break;
                case UserConstant.CUSTOMER_TYPE_DISTRIBUTOR:
                    buttonEffect = "distributorButtonEffect";
                    applyStateColumnName = "distributorApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_distributor_add_dsm*/
                    CuspostQuarterDistributorAddDsm distributorInfo1 = new CuspostQuarterDistributorAddDsm();
                    UpdateWrapper<CuspostQuarterDistributorAddDsm> distributorUpdateWrapper1 = new UpdateWrapper<>();
                    distributorUpdateWrapper1.set("applyStateCode", applyStateCode);
                    distributorUpdateWrapper1.set("approver", null);
                    distributorUpdateWrapper1.set("approvalOpinion", null);
                    distributorUpdateWrapper1.set("verifyRemark", null);
                    distributorUpdateWrapper1.eq("manageYear", manageYear);
                    distributorUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterDistributorAddDsmMapper.update(distributorInfo1, distributorUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_distributor_add_assistant*/
                    CuspostQuarterDistributorAddAssistant distributorInfo2 = new CuspostQuarterDistributorAddAssistant();
                    UpdateWrapper<CuspostQuarterDistributorAddAssistant> distributorUpdateWrapper2 = new UpdateWrapper<>();
                    distributorUpdateWrapper2.set("applyStateCode", applyStateCode);
                    distributorUpdateWrapper2.set("approver", null);
                    distributorUpdateWrapper2.set("approvalOpinion", null);
                    distributorUpdateWrapper2.set("verifyRemark", null);
                    distributorUpdateWrapper2.eq("manageYear", manageYear);
                    distributorUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterDistributorAddAssistantMapper.update(distributorInfo2, distributorUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_distributor_change_dsm*/
                    CuspostQuarterDistributorChangeDsm distributorInfo3 = new CuspostQuarterDistributorChangeDsm();
                    UpdateWrapper<CuspostQuarterDistributorChangeDsm> distributorUpdateWrapper3 = new UpdateWrapper<>();
                    distributorUpdateWrapper3.set("applyStateCode", applyStateCode);
                    distributorUpdateWrapper3.set("approver", null);
                    distributorUpdateWrapper3.set("approvalOpinion", null);
                    distributorUpdateWrapper3.set("verifyRemark", null);
                    distributorUpdateWrapper3.eq("manageYear", manageYear);
                    distributorUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterDistributorChangeDsmMapper.update(distributorInfo3, distributorUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_distributor_change_assistant*/
                    CuspostQuarterDistributorChangeAssistant distributorInfo4 = new CuspostQuarterDistributorChangeAssistant();
                    UpdateWrapper<CuspostQuarterDistributorChangeAssistant> distributorUpdateWrapper4 = new UpdateWrapper<>();
                    distributorUpdateWrapper4.set("applyStateCode", applyStateCode);
                    distributorUpdateWrapper4.set("approver", null);
                    distributorUpdateWrapper4.set("approvalOpinion", null);
                    distributorUpdateWrapper4.set("verifyRemark", null);
                    distributorUpdateWrapper4.eq("manageYear", manageYear);
                    distributorUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterDistributorChangeAssistantMapper.update(distributorInfo4, distributorUpdateWrapper4);

                    break;
                case UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ:
                    buttonEffect = "chainstoreHqButtonEffect";
                    applyStateColumnName = "chainstoreHqApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_dsm*/
                    CuspostQuarterChainstoreHqAddDsm chainstoreHqInfo1 = new CuspostQuarterChainstoreHqAddDsm();
                    UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> chainstoreHqUpdateWrapper1 = new UpdateWrapper<>();
                    chainstoreHqUpdateWrapper1.set("applyStateCode", applyStateCode);
                    chainstoreHqUpdateWrapper1.set("approver", null);
                    chainstoreHqUpdateWrapper1.set("approvalOpinion", null);
                    chainstoreHqUpdateWrapper1.set("verifyRemark", null);
                    chainstoreHqUpdateWrapper1.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterChainstoreHqAddDsmMapper.update(chainstoreHqInfo1, chainstoreHqUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_assistant*/
                    CuspostQuarterChainstoreHqAddAssistant chainstoreHqInfo2 = new CuspostQuarterChainstoreHqAddAssistant();
                    UpdateWrapper<CuspostQuarterChainstoreHqAddAssistant> chainstoreHqUpdateWrapper2 = new UpdateWrapper<>();
                    chainstoreHqUpdateWrapper2.set("applyStateCode", applyStateCode);
                    chainstoreHqUpdateWrapper2.set("approver", null);
                    chainstoreHqUpdateWrapper2.set("approvalOpinion", null);
                    chainstoreHqUpdateWrapper2.set("verifyRemark", null);
                    chainstoreHqUpdateWrapper2.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterChainstoreHqAddAssistantMapper.update(chainstoreHqInfo2, chainstoreHqUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_dsm*/
                    CuspostQuarterChainstoreHqChangeDsm chainstoreHqInfo3 = new CuspostQuarterChainstoreHqChangeDsm();
                    UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> chainstoreHqUpdateWrapper3 = new UpdateWrapper<>();
                    chainstoreHqUpdateWrapper3.set("applyStateCode", applyStateCode);
                    chainstoreHqUpdateWrapper3.set("approver", null);
                    chainstoreHqUpdateWrapper3.set("approvalOpinion", null);
                    chainstoreHqUpdateWrapper3.set("verifyRemark", null);
                    chainstoreHqUpdateWrapper3.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterChainstoreHqChangeDsmMapper.update(chainstoreHqInfo3, chainstoreHqUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_assistant*/
                    CuspostQuarterChainstoreHqChangeAssistant chainstoreHqInfo4 = new CuspostQuarterChainstoreHqChangeAssistant();
                    UpdateWrapper<CuspostQuarterChainstoreHqChangeAssistant> chainstoreHqUpdateWrapper4 = new UpdateWrapper<>();
                    chainstoreHqUpdateWrapper4.set("applyStateCode", applyStateCode);
                    chainstoreHqUpdateWrapper4.set("approver", null);
                    chainstoreHqUpdateWrapper4.set("approvalOpinion", null);
                    chainstoreHqUpdateWrapper4.set("verifyRemark", null);
                    chainstoreHqUpdateWrapper4.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterChainstoreHqChangeAssistantMapper.update(chainstoreHqInfo4, chainstoreHqUpdateWrapper4);

                    break;
            }

            /**更新申请编码状态 cuspost_quarter_apply_state_info*/
            CuspostQuarterApplyStateInfo info5 = new CuspostQuarterApplyStateInfo();
            UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper5 = new UpdateWrapper<>();
            updateWrapper5.set(applyStateColumnName, applyStateCode);
            updateWrapper5.eq("manageYear", manageYear);
            updateWrapper5.eq("manageQuarter", manageQuarter);
            updateWrapper5.eq("lvl2Code", region);
            int insertCount5 = cuspostQuarterApplyStateInfoMapper.update(info5, updateWrapper5);

            /**删除 cuspost_quarter_apply_state_region_info*/
            UpdateWrapper<CuspostQuarterApplyStateRegionInfo> updateWrapperDelete = new UpdateWrapper<>();
            updateWrapperDelete.eq("manageYear", manageYear);
            updateWrapperDelete.eq("manageQuarter", manageQuarter);
            updateWrapperDelete.eq("customerTypeCode", customerTypeCode);
            updateWrapperDelete.eq("region", region);
            int insertCountDelete = cuspostQuarterApplyStateRegionInfoMapper.delete(updateWrapperDelete);

            resultMap.put(buttonEffect, "0");

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 全部同意
     */
    @ApiOperation(value = "全部同意", notes = "全部同意")
    @RequestMapping(value = "/agreeAll", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper agreeAll(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear");                           // 年度
            String manageQuarter = object.getString("manageQuarter");                   // 季度
            String region = object.getString("region");                     // 大区
            String typeCode = object.getString("typeCode");                     // 新增，修改删除
            String customerTypeCode = object.getString("customerTypeCode");                     // 1医院，2零售，3商务，4连锁

            // 必须检查
            if (StringUtils.isEmpty(manageYear) || StringUtils.isEmpty(manageQuarter)
                    || StringUtils.isEmpty(region) || StringUtils.isEmpty(typeCode) || StringUtils.isEmpty(customerTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 大区状态表，判断这个人的等级和审批流编码，是否推给下个人或者结束
            //cuspost_quarter_apply_state_region_info
//            String roleName = customerPostMapper.getRoleNameByUserCode(loginUser.getUserCode());

            // 获取审批状态并更新
//            Map<String, String> map = customerPostMapper.getApprovalProcess(manageYear, manageQuarter, customerTypeCode, region, loginUser.getUserCode());
            Map<String, String> map = customerPostMapper.getApprovalProcess(manageYear, manageQuarter, customerTypeCode, region, loginUser.getRoleCode()); //20230215
            if (StringUtils.isEmpty(map)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "审批状态错误");
            }

            // 获取D&A审批人
//            String daName = customerPostMapper.getDaName();
            String daName = "D&A";

            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            int insertCount1 = 0;//insertCount
            int insertCount2 = 0;//insertCount
            int insertCount3 = 0;//insertCount
            int insertCount4 = 0;//insertCount
            String buttonEffect = "";//按钮是否有效
            String applyStateColumnName = "";//按钮是否有效
            switch (customerTypeCode) {
                case UserConstant.CUSTOMER_TYPE_HOSPITAL:
                    buttonEffect = "hospitalButtonEffect";
                    applyStateColumnName = "hospitalApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_hospital_add_dsm*/
                    CuspostQuarterHospitalAddDsm hospitalInfo1 = new CuspostQuarterHospitalAddDsm();
                    UpdateWrapper<CuspostQuarterHospitalAddDsm> hospitalUpdateWrapper1 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        hospitalUpdateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        hospitalUpdateWrapper1.set("approver", daName);
                    } else {
                        hospitalUpdateWrapper1.set("approver", map.get("nextStepUserName"));
                    }
                    hospitalUpdateWrapper1.eq("manageYear", manageYear);
                    hospitalUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterHospitalAddDsmMapper.update(hospitalInfo1, hospitalUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_hospital_add_assistant*/
                    CuspostQuarterHospitalAddAssistant hospitalInfo2 = new CuspostQuarterHospitalAddAssistant();
                    UpdateWrapper<CuspostQuarterHospitalAddAssistant> hospitalUpdateWrapper2 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        hospitalUpdateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        hospitalUpdateWrapper2.set("approver", daName);
                    } else {
                        hospitalUpdateWrapper2.set("approver", map.get("nextStepUserName"));
                    }
                    hospitalUpdateWrapper2.eq("manageYear", manageYear);
                    hospitalUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterHospitalAddAssistantMapper.update(hospitalInfo2, hospitalUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_hospital_change_dsm*/
                    CuspostQuarterHospitalChangeDsm hospitalInfo3 = new CuspostQuarterHospitalChangeDsm();
                    UpdateWrapper<CuspostQuarterHospitalChangeDsm> hospitalUpdateWrapper3 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        hospitalUpdateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        hospitalUpdateWrapper3.set("approver", daName);
                    } else {
                        hospitalUpdateWrapper3.set("approver", map.get("nextStepUserName"));
                    }
                    hospitalUpdateWrapper3.eq("manageYear", manageYear);
                    hospitalUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterHospitalChangeDsmMapper.update(hospitalInfo3, hospitalUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_hospital_change_assistant*/
                    CuspostQuarterHospitalChangeAssistant hospitalInfo4 = new CuspostQuarterHospitalChangeAssistant();
                    UpdateWrapper<CuspostQuarterHospitalChangeAssistant> hospitalUpdateWrapper4 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        hospitalUpdateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        hospitalUpdateWrapper4.set("approver", daName);
                    } else {
                        hospitalUpdateWrapper4.set("approver", map.get("nextStepUserName"));
                    }
                    hospitalUpdateWrapper4.eq("manageYear", manageYear);
                    hospitalUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    hospitalUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterHospitalChangeAssistantMapper.update(hospitalInfo4, hospitalUpdateWrapper4);

                    break;
                case UserConstant.CUSTOMER_TYPE_RETAIL:
                    buttonEffect = "retailButtonEffect";
                    applyStateColumnName = "retailApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_retail_add_dsm*/
                    CuspostQuarterRetailAddDsm retailInfo1 = new CuspostQuarterRetailAddDsm();
                    UpdateWrapper<CuspostQuarterRetailAddDsm> retailUpdateWrapper1 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        retailUpdateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        retailUpdateWrapper1.set("approver", daName);
                    } else {
                        retailUpdateWrapper1.set("approver", map.get("nextStepUserName"));
                    }
                    retailUpdateWrapper1.eq("manageYear", manageYear);
                    retailUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterRetailAddDsmMapper.update(retailInfo1, retailUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_retail_add_assistant*/
                    CuspostQuarterRetailAddAssistant retailInfo2 = new CuspostQuarterRetailAddAssistant();
                    UpdateWrapper<CuspostQuarterRetailAddAssistant> retailUpdateWrapper2 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        retailUpdateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        retailUpdateWrapper2.set("approver", daName);
                    } else {
                        retailUpdateWrapper2.set("approver", map.get("nextStepUserName"));
                    }
                    retailUpdateWrapper2.eq("manageYear", manageYear);
                    retailUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterRetailAddAssistantMapper.update(retailInfo2, retailUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_retail_change_dsm*/
                    CuspostQuarterRetailChangeDsm retailInfo3 = new CuspostQuarterRetailChangeDsm();
                    UpdateWrapper<CuspostQuarterRetailChangeDsm> retailUpdateWrapper3 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        retailUpdateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        retailUpdateWrapper3.set("approver", daName);
                    } else {
                        retailUpdateWrapper3.set("approver", map.get("nextStepUserName"));
                    }
                    retailUpdateWrapper3.eq("manageYear", manageYear);
                    retailUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterRetailChangeDsmMapper.update(retailInfo3, retailUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_retail_change_assistant*/
                    CuspostQuarterRetailChangeAssistant retailInfo4 = new CuspostQuarterRetailChangeAssistant();
                    UpdateWrapper<CuspostQuarterRetailChangeAssistant> retailUpdateWrapper4 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        retailUpdateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        retailUpdateWrapper4.set("approver", daName);
                    } else {
                        retailUpdateWrapper4.set("approver", map.get("nextStepUserName"));
                    }
                    retailUpdateWrapper4.eq("manageYear", manageYear);
                    retailUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    retailUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterRetailChangeAssistantMapper.update(retailInfo4, retailUpdateWrapper4);

                    break;
                case UserConstant.CUSTOMER_TYPE_DISTRIBUTOR:
                    buttonEffect = "distributorButtonEffect";
                    applyStateColumnName = "distributorApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_distributor_add_dsm*/
                    CuspostQuarterDistributorAddDsm distributorInfo1 = new CuspostQuarterDistributorAddDsm();
                    UpdateWrapper<CuspostQuarterDistributorAddDsm> distributorUpdateWrapper1 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        distributorUpdateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        distributorUpdateWrapper1.set("approver", daName);
                    } else {
                        distributorUpdateWrapper1.set("approver", map.get("nextStepUserName"));
                    }
                    distributorUpdateWrapper1.eq("manageYear", manageYear);
                    distributorUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterDistributorAddDsmMapper.update(distributorInfo1, distributorUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_distributor_add_assistant*/
                    CuspostQuarterDistributorAddAssistant distributorInfo2 = new CuspostQuarterDistributorAddAssistant();
                    UpdateWrapper<CuspostQuarterDistributorAddAssistant> distributorUpdateWrapper2 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        distributorUpdateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        distributorUpdateWrapper2.set("approver", daName);
                    } else {
                        distributorUpdateWrapper2.set("approver", map.get("nextStepUserName"));
                    }
                    distributorUpdateWrapper2.eq("manageYear", manageYear);
                    distributorUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterDistributorAddAssistantMapper.update(distributorInfo2, distributorUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_distributor_change_dsm*/
                    CuspostQuarterDistributorChangeDsm distributorInfo3 = new CuspostQuarterDistributorChangeDsm();
                    UpdateWrapper<CuspostQuarterDistributorChangeDsm> distributorUpdateWrapper3 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        distributorUpdateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        distributorUpdateWrapper3.set("approver", daName);
                    } else {
                        distributorUpdateWrapper3.set("approver", map.get("nextStepUserName"));
                    }
                    distributorUpdateWrapper3.eq("manageYear", manageYear);
                    distributorUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterDistributorChangeDsmMapper.update(distributorInfo3, distributorUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_distributor_change_assistant*/
                    CuspostQuarterDistributorChangeAssistant distributorInfo4 = new CuspostQuarterDistributorChangeAssistant();
                    UpdateWrapper<CuspostQuarterDistributorChangeAssistant> distributorUpdateWrapper4 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        distributorUpdateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        distributorUpdateWrapper4.set("approver", daName);
                    } else {
                        distributorUpdateWrapper4.set("approver", map.get("nextStepUserName"));
                    }
                    distributorUpdateWrapper4.eq("manageYear", manageYear);
                    distributorUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    distributorUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterDistributorChangeAssistantMapper.update(distributorInfo4, distributorUpdateWrapper4);

                    break;
                case UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ:
                    buttonEffect = "chainstoreHqButtonEffect";
                    applyStateColumnName = "chainstoreHqApplyStateCode";

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_dsm*/
                    CuspostQuarterChainstoreHqAddDsm chainstoreHqInfo1 = new CuspostQuarterChainstoreHqAddDsm();
                    UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> chainstoreHqUpdateWrapper1 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        chainstoreHqUpdateWrapper1.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        chainstoreHqUpdateWrapper1.set("approver", daName);
                    } else {
                        chainstoreHqUpdateWrapper1.set("approver", map.get("nextStepUserName"));
                    }
                    chainstoreHqUpdateWrapper1.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper1.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper1.eq("lvl2Code", region);
                    insertCount1 = cuspostQuarterChainstoreHqAddDsmMapper.update(chainstoreHqInfo1, chainstoreHqUpdateWrapper1);

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_assistant*/
                    CuspostQuarterChainstoreHqAddAssistant chainstoreHqInfo2 = new CuspostQuarterChainstoreHqAddAssistant();
                    UpdateWrapper<CuspostQuarterChainstoreHqAddAssistant> chainstoreHqUpdateWrapper2 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        chainstoreHqUpdateWrapper2.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        chainstoreHqUpdateWrapper2.set("approver", daName);
                    } else {
                        chainstoreHqUpdateWrapper2.set("approver", map.get("nextStepUserName"));
                    }
                    chainstoreHqUpdateWrapper2.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper2.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper2.eq("lvl2Code", region);
                    insertCount2 = cuspostQuarterChainstoreHqAddAssistantMapper.update(chainstoreHqInfo2, chainstoreHqUpdateWrapper2);

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_dsm*/
                    CuspostQuarterChainstoreHqChangeDsm chainstoreHqInfo3 = new CuspostQuarterChainstoreHqChangeDsm();
                    UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> chainstoreHqUpdateWrapper3 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        chainstoreHqUpdateWrapper3.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        chainstoreHqUpdateWrapper3.set("approver", daName);
                    } else {
                        chainstoreHqUpdateWrapper3.set("approver", map.get("nextStepUserName"));
                    }
                    chainstoreHqUpdateWrapper3.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper3.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper3.eq("lvl2Code", region);
                    insertCount3 = cuspostQuarterChainstoreHqChangeDsmMapper.update(chainstoreHqInfo3, chainstoreHqUpdateWrapper3);

                    /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_assistant*/
                    CuspostQuarterChainstoreHqChangeAssistant chainstoreHqInfo4 = new CuspostQuarterChainstoreHqChangeAssistant();
                    UpdateWrapper<CuspostQuarterChainstoreHqChangeAssistant> chainstoreHqUpdateWrapper4 = new UpdateWrapper<>();
                    if ("1".equals(map.get("isOver"))) {
                        chainstoreHqUpdateWrapper4.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
                        chainstoreHqUpdateWrapper4.set("approver", daName);
                    } else {
                        chainstoreHqUpdateWrapper4.set("approver", map.get("nextStepUserName"));
                    }
                    chainstoreHqUpdateWrapper4.eq("manageYear", manageYear);
                    chainstoreHqUpdateWrapper4.eq("manageQuarter", manageQuarter);
                    chainstoreHqUpdateWrapper4.eq("lvl2Code", region);
                    insertCount4 = cuspostQuarterChainstoreHqChangeAssistantMapper.update(chainstoreHqInfo4, chainstoreHqUpdateWrapper4);

                    break;
            }

            /**更新申请编码状态 cuspost_quarter_apply_state_info*/
            if ("1".equals(map.get("isOver"))) {
                CuspostQuarterApplyStateInfo info5 = new CuspostQuarterApplyStateInfo();
                UpdateWrapper<CuspostQuarterApplyStateInfo> updateWrapper5 = new UpdateWrapper<>();
                updateWrapper5.set(applyStateColumnName, UserConstant.APPLY_STATE_CODE_5);
                updateWrapper5.eq("manageYear", manageYear);
                updateWrapper5.eq("manageQuarter", manageQuarter);
                updateWrapper5.eq("lvl2Code", region);
                updateWrapper5.ne(applyStateColumnName, "00"); //20230616 本季不更新的数据不更新状态
                int insertCount5 = cuspostQuarterApplyStateInfoMapper.update(info5, updateWrapper5);

                //TODO 20230403 加入三方验证
                //TODO 20230425 hub_hco_hospital_opendata
                //TODO 20230425 hub_hco_retail_opendata
                //TODO 20230425 hub_dcr_application
                String fileId = commonUtils.createUUID();
                switch (customerTypeCode) {
                    case UserConstant.CUSTOMER_TYPE_HOSPITAL:
                        //插入新增
//                        customerPostMapper.insertQuarterThirdPartyHospitalAddFromHospital(manageYear,manageQuarter,region);
                        customerPostMapper.insertDcrFromQuarterHospitalAdd(manageYear, manageQuarter, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        //插入变更
//                        customerPostMapper.insertQuarterThirdPartyChangeHospitalFromHospital(manageYear,manageQuarter,manageMonth,region);
                        customerPostMapper.insertDcrFromQuarterHospitalChange(manageYear, manageQuarter, manageMonth, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        break;
                    case UserConstant.CUSTOMER_TYPE_RETAIL:
                        //插入新增
//                        customerPostMapper.insertQuarterThirdPartyNoHospitalAddFromRetail(manageYear,manageQuarter,region);
                        customerPostMapper.insertDcrFromQuarterRetailAdd(manageYear, manageQuarter, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        //插入变更
//                        customerPostMapper.insertQuarterThirdPartyChangeNoHospitalFromRetail(manageYear,manageQuarter,manageMonth,region);
                        customerPostMapper.insertDcrFromQuarterRetailChange(manageYear, manageQuarter, manageMonth, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        break;
                    case UserConstant.CUSTOMER_TYPE_DISTRIBUTOR:
                        //插入新增
//                        customerPostMapper.insertQuarterThirdPartyNoHospitalAddFromDistributor(manageYear,manageQuarter,region);
                        customerPostMapper.insertDcrFromQuarterDistributorAdd(manageYear, manageQuarter, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        //插入变更
//                        customerPostMapper.insertQuarterThirdPartyChangeNoHospitalFromDistributor(manageYear,manageQuarter,manageMonth,region);
                        customerPostMapper.insertDcrFromQuarterDistributorChange(manageYear, manageQuarter, manageMonth, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        break;
                    case UserConstant.CUSTOMER_TYPE_CHAINSTORE_HQ:
                        //插入新增
//                        customerPostMapper.insertQuarterThirdPartyNoHospitalAddFromChainstoreHq(manageYear,manageQuarter,region);
                        customerPostMapper.insertDcrFromQuarterChainstoreHqAdd(manageYear, manageQuarter, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        //插入变更
//                        customerPostMapper.insertQuarterThirdPartyChangeNoHospitalFromChainstoreHq(manageYear,manageQuarter,manageMonth,region);
                        customerPostMapper.insertDcrFromQuarterChainstoreHqChange(manageYear, manageQuarter, manageMonth, region, fileId, loginUser.getUserCode());// 20230425 三方验证逻辑变更

                        break;
                }

            }

            /**更新 cuspost_quarter_apply_state_region_info*/
            CuspostQuarterApplyStateRegionInfo cuspostInfo3 = new CuspostQuarterApplyStateRegionInfo();
            UpdateWrapper<CuspostQuarterApplyStateRegionInfo> updateWrapper = new UpdateWrapper<>();
            if ("1".equals(map.get("isOver"))) {
                updateWrapper.set("isOver", "1");
                updateWrapper.set("applyStateCode", UserConstant.APPLY_STATE_CODE_5);
            } else {
                updateWrapper.set("postCode", map.get("nextStepCode"));
            }
            updateWrapper.eq("manageYear", manageYear);
            updateWrapper.eq("manageQuarter", manageQuarter);
            updateWrapper.eq("customerTypeCode", customerTypeCode);
            updateWrapper.eq("region", region);
            int insertCount = cuspostQuarterApplyStateRegionInfoMapper.update(cuspostInfo3, updateWrapper);

            resultMap.put(buttonEffect, "0");

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    //endregion


    /****************************************************第三方**********************************************************/
    //region 第三方

    /**医院客户验证新增*/
    /**
     * 查询医院客户验证
     */
    @ApiOperation(value = "查询医院客户验证", notes = "查询医院客户验证")
    @RequestMapping(value = "/queryHospitalFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryHospitalFromThirdParty(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryHospitalFromThirdParty(page
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**非医院客户验证新增*/
    /**
     * 查询非医院客户验证
     */
    @ApiOperation(value = "查询非医院客户验证", notes = "查询非医院客户验证")
    @RequestMapping(value = "/queryNotHospitalFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryNotHospitalFromThirdParty(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryNotHospitalFromThirdParty(page
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载医院客户验证
     */
    @ApiOperation(value = "下载医院客户验证", notes = "下载医院客户验证")
    @RequestMapping(value = "/exprotHospitalFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotHospitalFromThirdParty(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryHospitalFromThirdParty(page
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_HOSPITAL);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件 downloadFileForZipWithDelete
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载非医院客户验证
     */
    @ApiOperation(value = "下载非医院客户验证", notes = "下载非医院客户验证")
    @RequestMapping(value = "/exprotNotHospitalFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotNotHospitalFromThirdParty(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryNotHospitalFromThirdParty(page
                    , manageYear, manageQuarter, customerName, applyStateCode
                    , province, city
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_NOT_HOSPITAL);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件 downloadFileForZipWithDelete
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 上传三方医院客户验证
     */
    @ApiOperation(value = "上传三方医院客户验证", notes = "上传三方医院客户验证")
    @RequestMapping("/batchAddHospitalFromThirdParty")
    @Transactional
    public Wrapper batchAddHospitalFromThirdParty(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
//            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
//            String manageQuarter = request.getParameter("manageQuarter");
//
//            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//            );
//            if (StringUtils.isEmpty(cuspostInfo)) {
//                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
//            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_HOSPITAL);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
//                String errorFileName = hospitalFromThirdPartyDataBatch("cuspost_quarter_third_party_hospital_add", uploadItemExplainModels,
//                        fileId, newFIleName, userCode, manageYear, manageQuarter);
                String errorFileName = hospitalFromThirdPartyDataBatch("cuspost_third_party_hospital_add", uploadItemExplainModels,
                        fileId, newFIleName, userCode);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
//    public String hospitalFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
    public String hospitalFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String manageMonth = commonUtils.getTodayYM2();

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, Integer.parseInt(manageMonth));

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
//                customerPostMapper.uploadHospitalFromThirdPartyYearQuarter(fileId, manageYear, manageQuarter);
                // 更新上传数据
                customerPostMapper.uploadHospitalFromThirdPartyUpdate(fileId, userCode);
                // 插入上传数据
//                customerPostMapper.uploadHospitalFromThirdPartyInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传非医院客户验证
     */
    @ApiOperation(value = "上传非医院客户验证", notes = "上传非医院客户验证")
    @RequestMapping("/batchAddNotHospitalFromThirdParty")
    @Transactional
    public Wrapper batchAddNotHospitalFromThirdParty(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
//            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
//            String manageQuarter = request.getParameter("manageQuarter");
//
//            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//            );
//            if (StringUtils.isEmpty(cuspostInfo)) {
//                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
//            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_NOT_HOSPITAL);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
//                String errorFileName = notHospitalFromThirdPartyDataBatch("cuspost_quarter_third_party_no_hospital_add", uploadItemExplainModels,
//                        fileId, newFIleName, userCode, manageYear, manageQuarter);
                String errorFileName = notHospitalFromThirdPartyDataBatch("cuspost_third_party_no_hospital_add", uploadItemExplainModels,
                        fileId, newFIleName, userCode);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
//    public String notHospitalFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
    public String notHospitalFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String manageMonth = commonUtils.getTodayYM2();

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, Integer.parseInt(manageMonth));

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
//                customerPostMapper.uploadNotHospitalFromThirdPartyYearQuarter(fileId, manageYear, manageQuarter);
                // 更新上传数据
                customerPostMapper.uploadNotHospitalFromThirdPartyUpdate(fileId, userCode);
                // 插入上传数据
//                customerPostMapper.uploadNotHospitalFromThirdPartyInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**医院客户验证变更删除*/

    /**
     * 查询医院客户验证
     */
    @ApiOperation(value = "查询医院客户验证", notes = "查询医院客户验证")
    @RequestMapping(value = "/queryHospitalChangeDeletionFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryHospitalChangeDeletionFromThirdParty(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
//            String manageYear = object.getString("manageYear"); // 年度
//            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
//            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
//            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//                            .eq("hospitalCheckBox", "1")
//            );
//            if (!StringUtils.isEmpty(cuspostInfo)) {
//                result = customerPostMapper.queryHospitalChangeDeletionFromThirdParty(page
//                        , manageYear, manageQuarter, manageMonth, customerName, applyStateCode
//                        , province, city
//                );
//            }
            IPage<Map<String, Object>> result = customerPostMapper.queryHospitalChangeDeletionFromThirdParty(page
                    , customerName, province, city
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = StringUtils.isEmpty(result) ? null : result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**非医院客户验证变更删除*/

    /**
     * 查询非医院客户验证
     */
    @ApiOperation(value = "查询非医院客户验证", notes = "查询非医院客户验证")
    @RequestMapping(value = "/queryNotHospitalChangeDeletionFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryNotHospitalChangeDeletionFromThirdParty(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
//            String manageYear = object.getString("manageYear"); // 年度
//            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
//            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);

            //查询是否有核心客岗关系表
//            String retailCheckBox = "";
//            String distributorCheckBox = "";
//            String chainstoreHqCheckBox = "";
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//            );
//            if (!StringUtils.isEmpty(cuspostInfo)) {
//                retailCheckBox = cuspostInfo.getRetailCheckBox();
//                distributorCheckBox = cuspostInfo.getDistributorCheckBox();
//                chainstoreHqCheckBox = cuspostInfo.getChainstoreHqCheckBox();
//            }
//
//            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
//            IPage<Map<String, Object>> result = customerPostMapper.queryNotHospitalChangeDeletionFromThirdParty(page
//                    , manageYear, manageQuarter, manageMonth, customerName, applyStateCode
//                    , province, city
//                    , retailCheckBox, distributorCheckBox, chainstoreHqCheckBox
//            );
            IPage<Map<String, Object>> result = customerPostMapper.queryNotHospitalChangeDeletionFromThirdParty(page
                    , customerName, province, city
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = StringUtils.isEmpty(result) ? null : result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载医院客户验证
     */
    @ApiOperation(value = "下载医院客户验证", notes = "下载医院客户验证")
    @RequestMapping(value = "/exprotHospitalChangeDeletionFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotHospitalChangeDeletionFromThirdParty(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
//            String manageYear = object.getString("manageYear"); // 年度
//            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
//            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);

            Page<Map<String, Object>> page = new Page<>(-1, -1);
//            IPage<Map<String, Object>> result = null;
            //查询是否有核心客岗关系表
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//                            .eq("hospitalCheckBox", "1")
//            );
//            if (!StringUtils.isEmpty(cuspostInfo)) {
//                result = customerPostMapper.queryHospitalChangeDeletionFromThirdParty(page
//                        , manageYear, manageQuarter, manageMonth, customerName, applyStateCode
//                        , province, city
//                );
//            }
            IPage<Map<String, Object>> result = customerPostMapper.queryHospitalChangeDeletionFromThirdParty(page
                    , customerName, province, city
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_HOSPITAL_CHANGE);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件 downloadFileForZipWithDelete
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 下载非医院客户验证
     */
    @ApiOperation(value = "下载非医院客户验证", notes = "下载非医院客户验证")
    @RequestMapping(value = "/exprotNotHospitalChangeDeletionFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotNotHospitalChangeDeletionFromThirdParty(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
//            String manageYear = object.getString("manageYear"); // 年度
//            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
//            String applyStateCode = object.getString("applyStateCode"); // 申请状态
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(Integer.parseInt(manageYear), manageQuarter);
//
//            //查询是否有核心客岗关系表
//            String retailCheckBox = "";
//            String distributorCheckBox = "";
//            String chainstoreHqCheckBox = "";
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//            );
//            if (!StringUtils.isEmpty(cuspostInfo)) {
//                retailCheckBox = cuspostInfo.getRetailCheckBox();
//                distributorCheckBox = cuspostInfo.getDistributorCheckBox();
//                chainstoreHqCheckBox = cuspostInfo.getChainstoreHqCheckBox();
//            }
//
            Page<Map<String, Object>> page = new Page<>(-1, -1);
//            IPage<Map<String, Object>> result = customerPostMapper.queryNotHospitalChangeDeletionFromThirdParty(page
//                    , manageYear, manageQuarter, manageMonth, customerName, applyStateCode
//                    , province, city
//                    , retailCheckBox, distributorCheckBox, chainstoreHqCheckBox
//            );
            IPage<Map<String, Object>> result = customerPostMapper.queryNotHospitalChangeDeletionFromThirdParty(page
                    , customerName, province, city
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_NOT_HOSPITAL_CHANGE_DELETION);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, StringUtils.isEmpty(result) ? null : result.getRecords());

            // 下载压缩文件 downloadFileForZipWithDelete
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 上传医院客户验证
     */
    @ApiOperation(value = "上传医院客户验证", notes = "上传医院客户验证")
    @RequestMapping("/batchAddHospitalChangeDeletionFromThirdParty")
    @Transactional
    public Wrapper batchAddHospitalChangeDeletionFromThirdParty(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
//            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
//            String manageQuarter = request.getParameter("manageQuarter");
//
//            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//            );
//            if (StringUtils.isEmpty(cuspostInfo)) {
//                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
//            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_HOSPITAL_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
//                String errorFileName = hospitalChangeDeletionFromThirdPartyDataBatch("cuspost_quarter_third_party_hospital_change", uploadItemExplainModels,
//                        fileId, newFIleName, userCode, manageYear, manageQuarter);
                String errorFileName = hospitalChangeDeletionFromThirdPartyDataBatch("cuspost_third_party_hospital_change", uploadItemExplainModels,
                        fileId, newFIleName, userCode);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
//    public String hospitalChangeDeletionFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
    public String hospitalChangeDeletionFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String manageMonth = commonUtils.getTodayYM2();

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, Integer.parseInt(manageMonth));

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
//                customerPostMapper.uploadHospitalChangeDeletionFromThirdPartyYearQuarter(fileId, manageYear, manageQuarter, manageMonth);
                // 更新上传数据
                customerPostMapper.uploadHospitalChangeDeletionFromThirdPartyUpdate(fileId, userCode);
                // 插入上传数据
//                customerPostMapper.uploadHospitalChangeDeletionFromThirdPartyInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传非医院客户验证
     */
    @ApiOperation(value = "上传非医院客户验证", notes = "上传非医院客户验证")
    @RequestMapping("/batchAddNotHospitalChangeDeletionFromThirdParty")
    @Transactional
    public Wrapper batchAddNotHospitalChangeDeletionFromThirdParty(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
//            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
//            String manageQuarter = request.getParameter("manageQuarter");
//
//            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
//            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
//                    new QueryWrapper<CuspostQuarterAdjustInfo>()
//                            .eq("manageYear", manageYear)
//                            .eq("manageQuarter", manageQuarter)
//            );
//            if (StringUtils.isEmpty(cuspostInfo)) {
//                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
//            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_NOT_HOSPITAL_CHANGE_DELETION);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
//                String errorFileName = notHospitalChangeDeletionFromThirdPartyDataBatch("cuspost_quarter_third_party_no_hospital_change", uploadItemExplainModels,
//                        fileId, newFIleName, userCode, manageYear, manageQuarter);
                String errorFileName = notHospitalChangeDeletionFromThirdPartyDataBatch("cuspost_third_party_no_hospital_change", uploadItemExplainModels,
                        fileId, newFIleName, userCode);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
//    public String notHospitalChangeDeletionFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
    public String notHospitalChangeDeletionFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            //生成下一季度第一个月字段
//            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            String manageMonth = commonUtils.getTodayYM2();

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, Integer.parseInt(manageMonth));

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
//                customerPostMapper.uploadNotHospitalChangeDeletionFromThirdPartyYearQuarter(fileId, manageYear, manageQuarter, manageMonth);
                // 更新上传数据
                customerPostMapper.uploadNotHospitalChangeDeletionFromThirdPartyUpdate(fileId, userCode);
                // 插入上传数据
//                customerPostMapper.uploadNotHospitalChangeDeletionFromThirdPartyInsert(fileId, userCode);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**三方区域验证*/
    /**
     * 查询区域验证
     */
    @ApiOperation(value = "查询区域验证", notes = "查询区域验证")
    @RequestMapping(value = "/queryAreaFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public Wrapper queryAreaFromThirdParty(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Integer pageSize = object.getInteger("rows"); // 每页显示数据量
            Integer nextPage = object.getInteger("page"); // 页数

            // 必须检查
            if (StringUtils.isEmpty(pageSize) || StringUtils.isEmpty(nextPage)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            // 检索处理
            Page<Map<String, Object>> page = new Page<>(nextPage, pageSize);
            IPage<Map<String, Object>> result = customerPostMapper.queryAreaFromThirdParty(page
                    , manageYear, manageQuarter, customerName, province, city
                    , orderName //20230302 排序
            );
            List<Map<String, Object>> list = result.getRecords();

            // 有值的场合
            if (!StringUtils.isEmpty(list) && list.size() > 0) {
                resultMap.put("totalPages", result.getPages());
                resultMap.put("currPage", result.getCurrent());
                resultMap.put("totalCount", result.getTotal());
            }

            resultMap.put("list", list);
        } catch (Exception e) {
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * 下载区域验证
     */
    @ApiOperation(value = "下载区域验证", notes = "下载区域验证")
    @RequestMapping(value = "/exprotAreaFromThirdParty", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    public void exprotAreaFromThirdParty(HttpServletRequest request, HttpServletResponse response, @RequestBody String json) {
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String manageYear = object.getString("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerName = object.getString("customerName"); // 客户名称
            String province = object.getString("province"); // 省份
            String city = object.getString("city"); // 城市
            String orderName = object.getString("orderName"); // 20230302 排序

            Page<Map<String, Object>> page = new Page<>(-1, -1);
            IPage<Map<String, Object>> result = customerPostMapper.queryAreaFromThirdParty(page
                    , manageYear, manageQuarter, customerName, province, city
                    , orderName //20230302 排序
            );

            // 生成下载Excel
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_HOSPITAL);
            List<UploadItemExplainModel> downItemExplainModelList = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsDownLoadItem())).collect(Collectors.toList());

            // 文件名做成
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            String fileName = "季度数据_" + df.format(new Date()) + ".xlsx";

            // 创建导出文件
            CustomerPostUtils customerPostUtils = new CustomerPostUtils();
            customerPostUtils.customerPostCreateExportFile(fileName, cusPostTemporaryPath, downItemExplainModelList, result.getRecords());

            // 下载压缩文件 downloadFileForZipWithDelete
            commonUtils.downloadFileWithDelete(request, fileName, cusPostTemporaryPath + fileName, response);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 上传三方区域验证新增
     */
    @ApiOperation(value = "上传三方区域验证新增", notes = "上传三方区域验证新增")
    @RequestMapping("/batchAddAreaFromThirdParty")
    @Transactional
    public Wrapper batchAddAreaFromThirdParty(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_THIRD_PARTY_AREA);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = areaAddFromThirdPartyDataBatch("cuspost_quarter_third_party_area", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String areaAddFromThirdPartyDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_YEAR_MONTH, manageMonth);

            /**校验 架构城市关系*/
            //区域验证只是验证零售数据
            String relation2 = customerPostMapper.updateCheckFromStructureCityByArea(
                    tableEnNameTem, nowYM, manageMonth, UserConstant.CUSTOMER_TYPE_RETAIL, fileId);
            if (relation2 != null) {
                String messageContent = " 客户 【" + relation2 + "】的架构城市关系不正确，请确认！";
                errorMessageList.add(messageContent);
            }


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //更新年度，季度
                customerPostMapper.uploadAreaFromThirdPartyYearQuarter(fileId, manageYear, manageQuarter, manageMonth, nowYM);
                // 更新上传数据
                customerPostMapper.uploadAreaFromThirdPartyUpdate(fileId, userCode);
                // 插入上传数据
                customerPostMapper.uploadAreaFromThirdPartyInsert(fileId, userCode);

                //更新正式库 医院，零售，商务，连锁
//                customerPostMapper.updateHubHcoHospitalFromThirdPartyArea(fileId);
                customerPostMapper.updateHubHcoRetailFromThirdPartyArea(fileId);
//                customerPostMapper.updateHubHcoDistributorFromThirdPartyArea(fileId);
//                customerPostMapper.updateHubHcoChainstoreHqFromThirdPartyArea(fileId);


            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    //endregion

    /*************************************************D&A审批***********************************************************/
    //region D&A审批

    /**
     * 上传零售终端新增审批结果
     */
    @ApiOperation(value = "上传零售终端新增审批结果", notes = "上传零售终端新增审批结果")
    @RequestMapping("/batchAddHospitalApplyQuarterFromDa")
    @Transactional
    public Wrapper batchAddHospitalApplyQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_HOSPITAL_APPLY);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = hospitalApplyQuarterFromDaDataBatch("cuspost_quarter_hospital_add_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String hospitalApplyQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 地区大区表查重（临时表已经有数据，check客户名称是否在地区经理表，大区助理表，核心表中存在，流向年月+客户名称）
            String cusNames2 = customerPostMapper.queryHospitalDaCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            /**校验 架构城市关系*/
            String relation2 = customerPostMapper.updateCheckFromStructureCity(
                    tableEnNameTem, nowYM, manageMonth, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE1, "1"); //20230222 D&A只check同意的数据
            if (relation2 != null) {
                String messageContent = " 客户 【" + relation2 + "】的架构城市关系不正确，请确认！";
                errorMessageList.add(messageContent);
            }

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterHospitalAddDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hco_category");
////                m2.put("columnValue","医院");
////                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","assigned_or_not");
////                m2.put("columnValue","已分配");
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_upstream_hp_id");
//                m2.put("columnValue",stringStringMap.get("upHospitalCode"));
//                m2.put("columnChName","上级医院CODE");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_upstream_hco_id");
//                m2.put("columnValue",stringStringMap.get("upCustomerCode"));
//                m2.put("columnChName","上级客户代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_region");
//                m2.put("columnValue",stringStringMap.get("region"));
//                m2.put("columnChName","大区");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hp_hp_otherflag");
////                m2.put("columnValue",stringStringMap.get("otherPropertyName"));
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_dsm_code");
//                m2.put("columnValue",stringStringMap.get("dsmCode"));
//                m2.put("columnChName","DSM岗位代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_dsm_cwid");
//                m2.put("columnValue",stringStringMap.get("dsmCwid"));
//                m2.put("columnChName","DSMCwid");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_dsm_name");
//                m2.put("columnValue",stringStringMap.get("dsmName"));
//                m2.put("columnChName","DSM名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_mr_code");
//                m2.put("columnValue",stringStringMap.get("repCode"));
//                m2.put("columnChName","Rep岗位代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_mr_cwid");
//                m2.put("columnValue",stringStringMap.get("repCwid"));
//                m2.put("columnChName","RepCwid");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_mr_name");
//                m2.put("columnValue",stringStringMap.get("repName"));
//                m2.put("columnChName","Rep名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_mr_products");
//                m2.put("columnValue",stringStringMap.get("territoryProducts"));
//                m2.put("columnChName","负责产品");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_drugstore_flag");
//                m2.put("columnValue",stringStringMap.get("drugstoreProperty1Name"));
//                m2.put("columnChName","药店属性1");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hp_target_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
////
////                m2 = new HashMap();
////                m2.put("columnEnName","hp_visit_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_hospital");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//
//            }
            //20240220 END

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaAddByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadHospitalApplyFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateHospitalDsmFromDaAdd(fileId);
                customerPostMapper.updateHospitalAssiFromDaAdd(fileId);

                // 插入上传数据（cuspost_quarter_hospital_add_da）
                customerPostMapper.uploadHospitalApplyFromDaInsert(fileId, userCode);

                // 插入上传数据，判断是同意还是驳回（hub_hco_hospital）
                customerPostMapper.deleteHcoHospitalByCusNameTon(fileId);// 20230414 主数据中dsm无值，也可以进行新增
                customerPostMapper.uploadHubHcoHospitalApplyFromDaInsert(fileId, userCode);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateHospitalAddDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateHospitalAddAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionHospitalAdd(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传零售终端新增审批结果
     */
    @ApiOperation(value = "上传零售终端新增审批结果", notes = "上传零售终端新增审批结果")
    @RequestMapping("/batchAddRetailApplyQuarterFromDa")
    @Transactional
    public Wrapper batchAddRetailApplyQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_RETAIL_APPLY);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = retailApplyQuarterFromDaDataBatch("cuspost_quarter_retail_add_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String retailApplyQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 核心表查重
            String cusNames2 = customerPostMapper.queryRetailDaCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterRetailAddDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hco_category");
////                m2.put("columnValue","药店");
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hco_type");
//                m2.put("columnValue",stringStringMap.get("propertyName"));
//                m2.put("columnChName","属性");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","assigned_or_not");
////                m2.put("columnValue","已分配");
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_upstream_hco_id");
//                m2.put("columnValue",stringStringMap.get("upCode"));
//                m2.put("columnChName","上级代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_upstream_hco_name");
//                m2.put("columnValue",stringStringMap.get("upName"));
//                m2.put("columnChName","上级名称");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","rt_target_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","rt_region");
////                m2.put("columnValue",stringStringMap.get("region"));
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_crm_code");
//                m2.put("columnValue",stringStringMap.get("crmCode"));
//                m2.put("columnChName","CRM编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_longitude");
//                m2.put("columnValue",stringStringMap.get("iongitude"));
//                m2.put("columnChName","经度");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_latitude");
//                m2.put("columnValue",stringStringMap.get("iatitude"));
//                m2.put("columnChName","维度");
//                l.add(m2);
//
//                //DSM,REP 不在D&A处更新
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_territory_sr_products");
//                m2.put("columnValue","All");
//                m2.put("columnChName","负责产品");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","rt_visit_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_drugstore_attr1");
//                m2.put("columnValue",stringStringMap.get("drugstoreProperty1Name"));
//                m2.put("columnChName","药店属性1");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_drugstore_attr2");
//                m2.put("columnValue",stringStringMap.get("drugstoreProperty2Name"));
//                m2.put("columnChName","药店属性2");
//                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_retail");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//            }
            //20240220 END

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaAddByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadRetailApplyFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateRetailDsmFromDaAdd(fileId);
                customerPostMapper.updateRetailAssiFromDaAdd(fileId);

                // 插入上传数据（cuspost_quarter_retail_add_da）
                customerPostMapper.uploadRetailApplyFromDaInsert(fileId, userCode);

                // 插入上传数据，判断是同意还是驳回（hub_hco_retail）
                customerPostMapper.deleteHcoRetailByCusNameTon(fileId);// 20230414 主数据中dsm无值，也可以进行新增
                customerPostMapper.uploadHubHcoRetailApplyFromDaInsert(fileId, userCode);

                // 20230406 Hazard 区域验证数据源逻辑调整
//                customerPostMapper.insertThirdPartyAreaAdd(fileId, userCode);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateRetailAddDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRetailAddAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionRetailAdd(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传商务新增审批结果
     */
    @ApiOperation(value = "上传商务新增审批结果", notes = "上传商务新增审批结果")
    @RequestMapping("/batchAddDistributorApplyQuarterFromDa")
    @Transactional
    public Wrapper batchAddDistributorApplyQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_DISTRIBUTOR_APPLY);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = distributorApplyQuarterFromDaDataBatch("cuspost_quarter_distributor_add_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String distributorApplyQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 核心表查重
            String cusNames2 = customerPostMapper.queryDistributorDaCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterDistributorAddDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hco_category");
////                m2.put("columnValue","商业");
////                l.add(m2);
////
////                m2 = new HashMap();
////                m2.put("columnEnName","assigned_or_not");
////                m2.put("columnValue","已分配");
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_grade");
//                m2.put("columnValue",stringStringMap.get("propertyName"));
//                m2.put("columnChName","属性");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","cmc_region");
////                m2.put("columnValue",stringStringMap.get("region"));
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_territory_dsm_code");
//                m2.put("columnValue",stringStringMap.get("dsmCode"));
//                m2.put("columnChName","DSM岗位代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_territory_dsm_cwid");
//                m2.put("columnValue",stringStringMap.get("dsmCwid"));
//                m2.put("columnChName","DSMCwid");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_territory_dsm_name");
//                m2.put("columnValue",stringStringMap.get("dsmName"));
//                m2.put("columnChName","负责人姓名");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_territory_products");
//                m2.put("columnValue","All");
//                m2.put("columnChName","负责产品");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hco_dadan_ornot");
////                m2.put("columnValue","是");
////                l.add(m2);
////
////                m2 = new HashMap();
////                m2.put("columnEnName","cmc_target_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
////
////                m2 = new HashMap();
////                m2.put("columnEnName","cmc_visit_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_distributor");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//            }
            //20240220 END

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaAddByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadDistributorApplyFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateDistributorDsmFromDaAdd(fileId);
                customerPostMapper.updateDistributorAssiFromDaAdd(fileId);

                // 插入上传数据（cuspost_quarter_distributor_add_da）
                customerPostMapper.uploadDistributorApplyFromDaInsert(fileId, userCode);

                // 插入上传数据，判断是同意还是驳回（hub_hco_distributor）
                customerPostMapper.deleteHcoDistributorByCusNameTon(fileId);// 20230414 主数据中dsm无值，也可以进行新增
                customerPostMapper.uploadHubHcoDistributorApplyFromDaInsert(fileId, userCode);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateDistributorAddDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateDistributorAddAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionDistributorAdd(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * 上传连锁新增审批结果
     */
    @ApiOperation(value = "上传连锁新增审批结果", notes = "上传连锁新增审批结果")
    @RequestMapping("/batchAddChainstoreHqApplyQuarterFromDa")
    @Transactional
    public Wrapper batchAddChainstoreHqApplyQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_CHAINSTORE_HQ_APPLY);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = chainstoreHqApplyQuarterFromDaDataBatch("cuspost_quarter_chainstore_hq_add_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String chainstoreHqApplyQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //check 核心表查重
            String cusNames2 = customerPostMapper.queryChainstoreHqDaCusDuplicateFromHubByTon(
                    tableEnNameTem, manageMonth, fileId);
            if (cusNames2 != null) {
                String messageContent = " 客户名称 【" + cusNames2 + "】在本季核心表中已存在，请确认！";
                errorMessageList.add(messageContent);
            }

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterChainstoreHqAddDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hco_category");
////                m2.put("columnValue","连锁总部");
////                l.add(m2);
////
////                m2 = new HashMap();
////                m2.put("columnEnName","assigned_or_not");
////                m2.put("columnValue","已分配");
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_dadan_flag");
//                m2.put("columnValue",stringStringMap.get("printListConditionName"));
//                m2.put("columnChName","打单情况");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_gongcang_hco_id");
//                m2.put("columnValue",stringStringMap.get("gongcangHcoId"));
//                m2.put("columnChName","共仓打单商业代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_gongcang_name");
//                m2.put("columnValue",stringStringMap.get("gongcangName"));
//                m2.put("columnChName","共仓打单商业名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_gongcang_grade");
//                m2.put("columnValue",stringStringMap.get("gongcangGrade"));
//                m2.put("columnChName","共仓商业级别");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_up_stream_le_cho_id");
//                m2.put("columnValue",stringStringMap.get("kaUpStreamLeChoId"));
//                m2.put("columnChName","归属上级编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_up_stream_le_name");
//                m2.put("columnValue",stringStringMap.get("kaUpStreamLeName"));
//                m2.put("columnChName","归属上级名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_rt_region");
//                m2.put("columnValue",stringStringMap.get("region"));
//                m2.put("columnChName","大区");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_telephone_email");
//                m2.put("columnValue",stringStringMap.get("telephone"));
//                m2.put("columnChName","电话");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_dsm_code");
//                m2.put("columnValue",stringStringMap.get("dsmCode"));
//                m2.put("columnChName","DSM岗位代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_dsm_cwid");
//                m2.put("columnValue",stringStringMap.get("dsmCwid"));
//                m2.put("columnChName","DSMCwid");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","ka_territory_dsm_name");
////                m2.put("columnValue",stringStringMap.get("dsmName"));
////                m2.put("columnChName","DSM岗位名称");
////                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_products");
//                m2.put("columnValue","All");
//                m2.put("columnChName","负责产品");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_ka_code");
//                m2.put("columnValue",stringStringMap.get("kaTerritoryKaCode"));
//                m2.put("columnChName","KA负责人岗位");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_ka_cwid");
//                m2.put("columnValue",stringStringMap.get("kaTerritoryKaCwid"));
//                m2.put("columnChName","KA负责人Cwid");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_ka_name");
//                m2.put("columnValue",stringStringMap.get("kaTerritoryKaName"));
//                m2.put("columnChName","KA负责人名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_tuozhan_code");
//                m2.put("columnValue",stringStringMap.get("kaTerritoryExpandCode"));
//                m2.put("columnChName","KA拓展经理岗位");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_tuozhan_cwid");
//                m2.put("columnValue",stringStringMap.get("kaTerritoryExpandCwid"));
//                m2.put("columnChName","拓展主任CWID");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_territory_tuozhan_name");
//                m2.put("columnValue",stringStringMap.get("kaTerritoryExpandName"));
//                m2.put("columnChName","拓展主任姓名");
//                l.add(m2);
//
////                m2 = new HashMap();
////                m2.put("columnEnName","hco_dadan_ornot");
////                m2.put("columnValue","是");
////                l.add(m2);
////
////                m2 = new HashMap();
////                m2.put("columnEnName","ka_target_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
////
////                m2 = new HashMap();
////                m2.put("columnEnName","ka_visit_or_not");
////                m2.put("columnValue","是");
////                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_chainstore_hq");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//            }
            //20240220 END

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaAddByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadChainstoreHqApplyFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateChainstoreHqDsmFromDaAdd(fileId);
                customerPostMapper.updateChainstoreHqAssiFromDaAdd(fileId);

                // 插入上传数据（cuspost_quarter_chainstore_hq_add_da）
                customerPostMapper.uploadChainstoreHqApplyFromDaInsert(fileId, userCode);

                // 插入上传数据，判断是同意还是驳回（hub_hco_chainstore_hq）
                customerPostMapper.deleteHcoChainstoreHqByCusNameTon(fileId);// 20230414 主数据中dsm无值，也可以进行新增
                customerPostMapper.uploadHubHcoChainstoreHqApplyFromDaInsert(fileId, userCode);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateChainstoreHqAddDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateChainstoreHqAddAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionChainstoreHqAdd(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * @MethodName 客户名称历史信息检查
     * @Remark 按标准名称，24各月内出现过，提示“两年内删除过”，24个月外出现过，历史编码：XXXXXXX
     * D&A审批新增，变更 前校验用
     * @Authror Hazard
     * @Date 2023/1/12 15:37
     */
    @ApiOperation(value = "客户名称历史信息检查", notes = "客户名称历史信息检查")
    @RequestMapping(value = "/checkCoreHistoryCustomerName", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper checkCoreHistoryCustomerName(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();

        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            String customerTypeCode = object.getString("customerTypeCode"); // 客户类型(1医院,2零售,3商务,4连锁)
            String customerName = object.getString("customerName"); // 客户名称

            //获取年月时间
            int nowYM = Integer.parseInt(commonUtils.getTodayYM2());
            int twoYearYM = Integer.parseInt(commonUtils.getDynamicYm("-", 23));
            int twoYearOtherYM = Integer.parseInt(commonUtils.getDynamicYm("-", 24));

            List<Map<String, String>> twoYearList = new ArrayList<>();
            List<Map<String, String>> twoYearOtherList = new ArrayList<>();

            //查询两年内数据
            switch (customerTypeCode) {
                case "1":
                    twoYearList = customerPostMapper.queryHistoryHospitalCusNameFromHub(twoYearYM, nowYM, customerName);
                    break;
                case "2":
                    twoYearList = customerPostMapper.queryHistoryRetailCusNameFromHub(twoYearYM, nowYM, customerName);
                    break;
                case "3":
                    twoYearList = customerPostMapper.queryHistoryDistributorCusNameFromHub(twoYearYM, nowYM, customerName);
                    break;
                case "4":
                    twoYearList = customerPostMapper.queryHistoryChainstoreHqCusNameFromHub(twoYearYM, nowYM, customerName);
                    break;
                default:
                    break;
            }

            if (twoYearList.size() > 0) {
                resultMap.put("errorMessage", "两年内删除过");
            } else {
                //查询两年外数据
                switch (customerTypeCode) {
                    case "1":
                        twoYearOtherList = customerPostMapper.queryHistoryHospitalCusNameFromHub(200001, twoYearOtherYM, customerName);
                        break;
                    case "2":
                        twoYearOtherList = customerPostMapper.queryHistoryRetailCusNameFromHub(200001, twoYearOtherYM, customerName);
                        break;
                    case "3":
                        twoYearOtherList = customerPostMapper.queryHistoryDistributorCusNameFromHub(200001, twoYearOtherYM, customerName);
                        break;
                    case "4":
                        twoYearOtherList = customerPostMapper.queryHistoryChainstoreHqCusNameFromHub(200001, twoYearOtherYM, customerName);
                        break;
                    default:
                        break;
                }
                if (twoYearOtherList.size() > 0) {
                    resultMap.put("errorMessage", "历史编码：" + twoYearOtherList.get(0).get("customerCode"));
                } else {
                    resultMap.put("errorMessage", "无异常");
                }
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            logger.info(e.getMessage());
            logger.info(e.getStackTrace());
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * D&A医院申请审批
     */
    @ApiOperation(value = "D&A医院申请审批", notes = "D&A医院申请审批")
    @RequestMapping(value = "/approveHospitalApplyQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveHospitalApplyQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String applyCode = object.getString("applyCode"); //申请编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String customerCode = object.getString("customerCode"); //客户编码
            String crmCode = object.getString("crmCode"); //CRM编码
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); //药店属性1
            String sameTimeRetailCode = object.getString("sameTimeRetailCode"); //同时为零售终端
            String otherPropertyCode = object.getString("otherPropertyCode"); //其他属性
            String upHospitalCode = object.getString("upHospitalCode"); //上级医院CODE
            String dsmCode = object.getString("dsmCode"); //DSM岗位代码
            String repCode = object.getString("repCode"); //REP岗位代码
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String region = object.getString("region"); //大区
            //20230529 START
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String upCustomerCode = object.getString("upCustomerCode");                   // 上级客户代码
            //20230529 END

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //与已有主数据进行查重
            if ("1".equals(approveTypeCode)) { //同意的场合
//                int count2 = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, null, customerName);
                int count2 = customerPostMapper.queryHospitalDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
                if (count2 > 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "在本季核心表中已存在，请确认！");
                }
            }

            /**校验 架构城市关系*/
            int countFromStructureCity = customerPostMapper.queryCountFromStructureCity(nowYM, repCode, city);
            if (countFromStructureCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "架构城市关系错误", "架构城市关系不正确！");
            }

            /**获取额外的信息内容*/
            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //获取repName,repCwid
            Map<String, String> repMap = customerPostMapper.getDataNameByDataCode(nowYM, repCode);
            String repName = null;
            String repCwid = null;
            if (!StringUtils.isEmpty(repMap)) {
                repName = repMap.get("userName");
                repCwid = repMap.get("cwid");
            } else {
                //架构错误
            }

            // 药店属性1
            String drugstoreProperty1Name = null;
            if (!StringUtils.isEmpty(drugstoreProperty1Code)) {
                List<MasterDictionaryInfo> list0005 = masterCommonMapper.getMasterDictionaryInfoByType("MST0005", drugstoreProperty1Code);
                if (list0005.size() > 0) {
                    drugstoreProperty1Name = list0005.get(0).getDicValue();
                }
            }

            // 同时为零售终端
            String sameTimeRetailName = null;
            if (!StringUtils.isEmpty(sameTimeRetailCode)) {
                List<MasterDictionaryInfo> list0000 = masterCommonMapper.getMasterDictionaryInfoByType("MST0000", sameTimeRetailCode);
                if (list0000.size() > 0) {
                    sameTimeRetailName = list0000.get(0).getDicValue();
                }
            }

            // 其他属性
            String otherPropertyName = null;
            if (!StringUtils.isEmpty(otherPropertyCode)) {
                List<MasterDictionaryInfo> list0004 = masterCommonMapper.getMasterDictionaryInfoByType("MST0004", otherPropertyCode);
                if (list0004.size() > 0) {
                    otherPropertyName = list0004.get(0).getDicValue();
                }
            }

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hco_category");
//            m2.put("columnValue","医院");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","assigned_or_not");
//            m2.put("columnValue","已分配");
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_upstream_hp_id");
            m2.put("columnValue",upHospitalCode);
            m2.put("columnChName","上级医院CODE");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_upstream_hco_id");
            m2.put("columnValue",upCustomerCode);
            m2.put("columnChName","上级客户代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_region");
            m2.put("columnValue",region);
            m2.put("columnChName","大区");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_crm_code");
            m2.put("columnValue",crmCode);
            m2.put("columnChName","CRM编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hp_hp_otherflag");
//            m2.put("columnValue",otherPropertyName);
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_dsm_code");
            m2.put("columnValue",dsmCode);
            m2.put("columnChName","DSM岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_dsm_cwid");
            m2.put("columnValue",dsmCwid);
            m2.put("columnChName","DSMCwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_dsm_name");
            m2.put("columnValue",dsmName);
            m2.put("columnChName","DSM名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_mr_code");
            m2.put("columnValue",repCode);
            m2.put("columnChName","Rep岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_mr_cwid");
            m2.put("columnValue",repCwid);
            m2.put("columnChName","RepCwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_mr_name");
            m2.put("columnValue",repName);
            m2.put("columnChName","Rep名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_mr_products");
            m2.put("columnValue",territoryProducts);
            m2.put("columnChName","负责产品");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_drugstore_flag");
            m2.put("columnValue",drugstoreProperty1Name);
            m2.put("columnChName","药店属性1");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hp_target_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","hp_visit_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_hospital");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_hospital_add_da
            CuspostQuarterHospitalAddDa info = new CuspostQuarterHospitalAddDa();
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setApplyCode(applyCode);
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
            info.setRegion(region);
            info.setCrmCode(crmCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setSameTimeRetailCode(sameTimeRetailCode);
            info.setSameTimeRetailName(sameTimeRetailName);
            info.setOtherPropertyCode(otherPropertyCode);
            info.setOtherPropertyName(otherPropertyName);
            info.setUpHospitalCode(upHospitalCode);
            info.setDrugstoreProperty1Code(drugstoreProperty1Code);
            info.setDrugstoreProperty1Name(drugstoreProperty1Name);
            info.setDsmCode(dsmCode);
            info.setDsmCwid(dsmCwid);
            info.setDsmName(dsmName);
            info.setRepCode(repCode);
            info.setRepCwid(repCwid);
            info.setRepName(repName);
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            //20230529 START
            info.setTerritoryProducts(territoryProducts);
            info.setUpCustomerCode(upCustomerCode);
            //20230529 END
            int insertCount = cuspostQuarterHospitalAddDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_hospital_add_dsm*/
            CuspostQuarterHospitalAddDsm info1 = new CuspostQuarterHospitalAddDsm();
            UpdateWrapper<CuspostQuarterHospitalAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("customerCode", customerCode); //20230117
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.set("approver", "");//20230529 20230621
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterHospitalAddDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_hospital_add_assistant*/
            CuspostQuarterHospitalAddAssistant info2 = new CuspostQuarterHospitalAddAssistant();
            UpdateWrapper<CuspostQuarterHospitalAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("customerCode", customerCode); //20230117
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.set("approver", "");//20230529 20230621
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterHospitalAddAssistantMapper.update(info2, updateWrapper2);

            //20230519 D&A审批更新后，一览状态逻辑判断

            // 更新到正式库
            if ("1".equals(approveTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int delFlag = customerPostMapper.deleteHcoHospitalByCusName(model);// 20230414 主数据中dsm无值，也可以进行新增
                int f = customerPostMapper.insertHcoHospitalFromDa(model);
            }

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateHospitalAddDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateHospitalAddAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionHospitalAdd(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            logger.info(e.getMessage());
            logger.info(e.getStackTrace());
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * D&A零售终端申请审批
     */
    @ApiOperation(value = "D&A零售终端申请审批", notes = "D&A零售终端申请审批")
    @RequestMapping(value = "/approveRetailApplyQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveRetailApplyQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String applyCode = object.getString("applyCode"); //申请编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String customerCode = object.getString("customerCode"); //客户编码
            String crmCode = object.getString("crmCode"); //CRM编码
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
            String propertyCode = object.getString("propertyCode"); //属性
            String upCode = object.getString("upCode"); //上级代码
            String upName = object.getString("upName"); //上级名称
            String drugstoreProperty1Code = object.getString("drugstoreProperty1Code"); //药店属性1
            String drugstoreProperty2Code = object.getString("drugstoreProperty2Code"); //药店属性2
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String region = object.getString("region"); //大区
            //20230529 START
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            //20230529 END

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //与已有主数据进行查重
            if ("1".equals(approveTypeCode)) { //同意的场合
//                int count2 = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, null, customerName);
                int count2 = customerPostMapper.queryRetailDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
                if (count2 > 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "在本季核心表中已存在，请确认！");
                }
            }

            // 属性
            String propertyName = null;
            if (!StringUtils.isEmpty(propertyCode)) {
                List<MasterDictionaryInfo> list0006 = masterCommonMapper.getMasterDictionaryInfoByType("MST0006", propertyCode);
                if (list0006.size() > 0) {
                    propertyName = list0006.get(0).getDicValue();
                }
            }

            // 药店属性1
            String drugstoreProperty1Name = null;
            if (!StringUtils.isEmpty(drugstoreProperty1Code)) {
                List<MasterDictionaryInfo> list0005 = masterCommonMapper.getMasterDictionaryInfoByType("MST0005", drugstoreProperty1Code);
                if (list0005.size() > 0) {
                    drugstoreProperty1Name = list0005.get(0).getDicValue();
                }
            }

            // 药店属性2
            String drugstoreProperty2Name = null;
            if (!StringUtils.isEmpty(drugstoreProperty2Code)) {
                List<MasterDictionaryInfo> list0012 = masterCommonMapper.getMasterDictionaryInfoByType("MST0012", drugstoreProperty2Code);
                if (list0012.size() > 0) {
                    drugstoreProperty2Name = list0012.get(0).getDicValue();
                }
            }

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hco_category");
//            m2.put("columnValue","药店");
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hco_type");
            m2.put("columnValue",propertyName);
            m2.put("columnChName","属性");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","assigned_or_not");
//            m2.put("columnValue","已分配");
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_upstream_hco_id");
            m2.put("columnValue",upCode);
            m2.put("columnChName","上级代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_upstream_hco_name");
            m2.put("columnValue",upName);
            m2.put("columnChName","上级名称");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","rt_target_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_region");
            m2.put("columnValue",region);
            m2.put("columnChName","大区");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_crm_code");
            m2.put("columnValue",crmCode);
            m2.put("columnChName","CRM编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_longitude");
            m2.put("columnValue",iongitude);
            m2.put("columnChName","经度");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_latitude");
            m2.put("columnValue",iatitude);
            m2.put("columnChName","维度");
            l.add(m2);

            //DSM,REP 不在D&A处更新

            m2 = new HashMap();
            m2.put("columnEnName","rt_territory_sr_products");
            m2.put("columnValue",territoryProducts);
            m2.put("columnChName","负责产品");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","rt_visit_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_drugstore_attr1");
            m2.put("columnValue",drugstoreProperty1Name);
            m2.put("columnChName","药店属性1");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_drugstore_attr2");
            m2.put("columnValue",drugstoreProperty2Name);
            m2.put("columnChName","药店属性2");
            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_retail");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_retail_add_da
            CuspostQuarterRetailAddDa info = new CuspostQuarterRetailAddDa();
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setApplyCode(applyCode);
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
//            info.setRegion(region);
            info.setCrmCode(crmCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setPropertyCode(propertyCode);
            info.setPropertyName(propertyName);
            info.setUpCode(upCode);
            info.setUpName(upName);
            info.setDrugstoreProperty1Code(drugstoreProperty1Code);
            info.setDrugstoreProperty1Name(drugstoreProperty1Name);
            info.setDrugstoreProperty2Code(drugstoreProperty2Code);
            info.setDrugstoreProperty2Name(drugstoreProperty2Name);
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            //20230529 START
            info.setTerritoryProducts(territoryProducts);
            //20230529 END
            int insertCount = cuspostQuarterRetailAddDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_retail_add_dsm*/
            CuspostQuarterRetailAddDsm info1 = new CuspostQuarterRetailAddDsm();
            UpdateWrapper<CuspostQuarterRetailAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("customerCode", customerCode); //20230117
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.set("approver", "");//20230529 20230621
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterRetailAddDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_retail_add_assistant*/
            CuspostQuarterRetailAddAssistant info2 = new CuspostQuarterRetailAddAssistant();
            UpdateWrapper<CuspostQuarterRetailAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("customerCode", customerCode); //20230117
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.set("approver", "");//20230529 20230621
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterRetailAddAssistantMapper.update(info2, updateWrapper2);

            // 更新到正式库
            if ("1".equals(approveTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int delFlag = customerPostMapper.deleteHcoRetailByCusName(model);// 20230414 主数据中dsm无值，也可以进行新增
                int f = customerPostMapper.insertHcoRetailFromDa(model);
            }

            // 20230406 Hazard 区域验证数据源逻辑调整
//            CuspostQuarterThirdPartyArea thirdPartyArea = new CuspostQuarterThirdPartyArea();
//            thirdPartyArea.setManageYear(BigDecimal.valueOf(manageYear));
//            thirdPartyArea.setManageQuarter(manageQuarter);
//            thirdPartyArea.setYearMonth(BigDecimal.valueOf(manageMonth));
//            thirdPartyArea.setCustomerCode(customerCode);
//            thirdPartyArea.setCustomerName(customerName);
//            thirdPartyArea.setProvince(province);
//            thirdPartyArea.setCity(city);
//            thirdPartyArea.setAddress(address);
//            thirdPartyArea.setInsertUser(loginUser.getUserCode());
//            thirdPartyArea.setInsertTime(new Date());
//            cuspostQuarterThirdPartyAreaMapper.insert(thirdPartyArea);

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateRetailAddDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRetailAddAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionRetailAdd(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            logger.info(e.getMessage());
            logger.info(e.getStackTrace());
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * D&A商务申请审批
     */
    @ApiOperation(value = "D&A商务申请审批", notes = "D&A商务申请审批")
    @RequestMapping(value = "/approveDistributorApplyQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveDistributorApplyQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String applyCode = object.getString("applyCode"); //申请编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String customerCode = object.getString("customerCode"); //客户编码
            String crmCode = object.getString("crmCode"); //CRM编码
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
            String propertyCode = object.getString("propertyCode"); //属性
            String dsmCode = object.getString("dsmCode"); //DSM岗位代码
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String region = object.getString("region"); //大区
            //20230529 START
            String telephone = object.getString("telephone");                   // 经销商电话
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String territoryDsmMp = object.getString("territoryDsmMp");                   // 联系电话
            String territoryDsmName = object.getString("territoryDsmName");                   // 负责人姓名
            String contactsAddress = object.getString("contactsAddress");                   // 经销商地址
            String contactsNamenphone = object.getString("contactsNamenphone");                   // 电话/人
            //20230529 END

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //与已有主数据进行查重
            if ("1".equals(approveTypeCode)) { //同意的场合
//                int count2 = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, null, customerName);
                int count2 = customerPostMapper.queryDistributorDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
                if (count2 > 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "在本季核心表中已存在，请确认！");
                }
            }

            /**获取额外的信息内容*/
            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            // 其他属性
            String propertyName = null;
            if (!StringUtils.isEmpty(propertyCode)) {
                List<MasterDictionaryInfo> list0007 = masterCommonMapper.getMasterDictionaryInfoByType("MST0007", propertyCode);
                if (list0007.size() > 0) {
                    propertyName = list0007.get(0).getDicValue();
                }
            }

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hco_category");
//            m2.put("columnValue","商业");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","assigned_or_not");
//            m2.put("columnValue","已分配");
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_grade");
            m2.put("columnValue",propertyName);
            m2.put("columnChName","属性");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_telephone");
            m2.put("columnValue",telephone);
            m2.put("columnChName","电话");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_region");
            m2.put("columnValue",region);
            m2.put("columnChName","大区");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_crm_code");
            m2.put("columnValue",crmCode);
            m2.put("columnChName","CRM编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_territory_dsm_code");
            m2.put("columnValue",dsmCode);
            m2.put("columnChName","DSM岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_territory_dsm_cwid");
            m2.put("columnValue",dsmCwid);
            m2.put("columnChName","DSMCwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_territory_dsm_mp");
            m2.put("columnValue",territoryDsmMp);
            m2.put("columnChName","联系电话");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_territory_dsm_name");
            m2.put("columnValue",territoryDsmName);
            m2.put("columnChName","DSM名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_territory_products");
            m2.put("columnValue",territoryProducts);
            m2.put("columnChName","负责产品");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_contacts_namenphone");
            m2.put("columnValue",contactsNamenphone);
            m2.put("columnChName","电话、人");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_contacts_address");
            m2.put("columnValue",contactsAddress);
            m2.put("columnChName","经销商地址");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hco_dadan_ornot");
//            m2.put("columnValue","是");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","cmc_target_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","cmc_visit_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_distributor");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_hospital_add_da
            CuspostQuarterDistributorAddDa info = new CuspostQuarterDistributorAddDa();
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setApplyCode(applyCode);
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
            info.setCrmCode(crmCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setPropertyCode(propertyCode);
            info.setPropertyName(propertyName);
            info.setDsmCode(dsmCode);
            info.setDsmCwid(dsmCwid);
            info.setDsmName(dsmName);
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            //20230529 START
            info.setTelephone(telephone);
            info.setTerritoryProducts(territoryProducts);
            info.setTerritoryDsmMp(territoryDsmMp);
            info.setTerritoryDsmName(territoryDsmName);
            info.setContactsAddress(contactsAddress);
            info.setContactsNamenphone(contactsNamenphone);
            //20230529 END
            int insertCount = cuspostQuarterDistributorAddDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_distributor_add_dsm*/
            CuspostQuarterDistributorAddDsm info1 = new CuspostQuarterDistributorAddDsm();
            UpdateWrapper<CuspostQuarterDistributorAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("customerCode", customerCode); //20230117
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.set("approver", "");//20230529 20230621
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterDistributorAddDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_distributor_add_assistant*/
            CuspostQuarterDistributorAddAssistant info2 = new CuspostQuarterDistributorAddAssistant();
            UpdateWrapper<CuspostQuarterDistributorAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("customerCode", customerCode); //20230117
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.set("approver", "");//20230529 20230621
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterDistributorAddAssistantMapper.update(info2, updateWrapper2);

            // 更新到正式库
            if ("1".equals(approveTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int delFlag = customerPostMapper.deleteHcoDistributorByCusName(model);// 20230414 主数据中dsm无值，也可以进行新增
                int f = customerPostMapper.insertHcoDistributorFromDa(model);
            }

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateDistributorAddDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateDistributorAddAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionDistributorAdd(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            logger.info(e.getMessage());
            logger.info(e.getStackTrace());
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * D&A连锁申请审批
     */
    @ApiOperation(value = "D&A连锁申请审批", notes = "D&A连锁申请审批")
    @RequestMapping(value = "/approveChainstoreHqApplyQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveChainstoreHqApplyQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        String nowYM = commonUtils.getTodayYM2();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String applyCode = object.getString("applyCode"); //申请编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String customerCode = object.getString("customerCode"); //客户编码
            String crmCode = object.getString("crmCode"); //CRM编码
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
            String printListConditionCode = object.getString("printListConditionCode"); //打单情况
            String dsmCode = object.getString("dsmCode"); //DSM岗位代码
            String kaTerritoryKaCode = object.getString("kaTerritoryKaCode");           // KA负责人岗位
            String kaTerritoryExpandCode = object.getString("kaTerritoryExpandCode");   // KA拓展经理岗位
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String region = object.getString("region"); //大区
            //20230529 START
            String telephone = object.getString("telephone");                   // 电话
            String kaUpStreamLeChoId = object.getString("kaUpStreamLeChoId");                   // 归属上级编码
            String kaUpStreamLeName = object.getString("kaUpStreamLeName");                   // 归属上级名称
            String territoryProducts = object.getString("territoryProducts");                   // 负责产品
            String territoryTuozhanCountby = object.getString("territoryTuozhanCountby");       // 计量方式
            String gongcangHcoId = object.getString("gongcangHcoId");                   // 共仓打单商业代码
            String gongcangName = object.getString("gongcangName");                   // 共仓打单商业名称
            String gongcangGrade = object.getString("gongcangGrade");                   // 共仓商业级别
            //20230529 END

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);
            //与已有主数据进行查重
            if ("1".equals(approveTypeCode)) { //同意的场合
//                int count2 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, null, customerName);
                int count2 = customerPostMapper.queryChainstoreHqDsmCusDuplicateFromHub(manageMonth, "1", null, customerName, null);// 20230414 主数据中dsm无值，也可以进行新增
                if (count2 > 0) {
                    return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "重复错误", "在本季核心表中已存在，请确认！");
                }
            }

            /**获取额外的信息内容*/
            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //获取kaName,kaCwid
            Map<String, String> kaMap = customerPostMapper.getDataNameByDataCode(nowYM, kaTerritoryKaCode);
            String kaName = null;
            String kaCwid = null;
            if (!StringUtils.isEmpty(kaMap)) {
                kaName = kaMap.get("userName");
                kaCwid = kaMap.get("cwid");
            } else {
                //架构错误
            }

            //获取kaExName,kaExCwid
            Map<String, String> kaExMap = customerPostMapper.getDataNameByDataCode(nowYM, kaTerritoryExpandCode);
            String kaExName = null;
            String kaExCwid = null;
            if (!StringUtils.isEmpty(kaExMap)) {
                kaExName = kaExMap.get("userName");
                kaExCwid = kaExMap.get("cwid");
            } else {
                //架构错误
            }

            // 其他属性
            String printListConditionName = null;
            if (!StringUtils.isEmpty(printListConditionCode)) {
                List<MasterDictionaryInfo> list0008 = masterCommonMapper.getMasterDictionaryInfoByType("MST0008", printListConditionCode);
                if (list0008.size() > 0) {
                    printListConditionName = list0008.get(0).getDicValue();
                }
            }

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_crm_code");
            m2.put("columnValue",crmCode);
            m2.put("columnChName","CRM编码");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hco_category");
//            m2.put("columnValue","连锁总部");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","assigned_or_not");
//            m2.put("columnValue","已分配");
//            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_dadan_flag");
            m2.put("columnValue",printListConditionName);
            m2.put("columnChName","打单情况");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_gongcang_hco_id");
            m2.put("columnValue",gongcangHcoId);
            m2.put("columnChName","共仓打单商业代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_gongcang_name");
            m2.put("columnValue",gongcangName);
            m2.put("columnChName","共仓打单商业名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_gongcang_grade");
            m2.put("columnValue",gongcangGrade);
            m2.put("columnChName","共仓商业级别");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_up_stream_le_cho_id");
            m2.put("columnValue",kaUpStreamLeChoId);
            m2.put("columnChName","归属上级编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_up_stream_le_name");
            m2.put("columnValue",kaUpStreamLeName);
            m2.put("columnChName","归属上级名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_rt_region");
            m2.put("columnValue",region);
            m2.put("columnChName","大区");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_telephone_email");
            m2.put("columnValue",telephone);
            m2.put("columnChName","电话");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_dsm_code");
            m2.put("columnValue",dsmCode);
            m2.put("columnChName","DSM岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_dsm_cwid");
            m2.put("columnValue",dsmCwid);
            m2.put("columnChName","DSMCwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_dsm_name");
            m2.put("columnValue",dsmName);
            m2.put("columnChName","DSM岗位名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_products");
            m2.put("columnValue",territoryProducts);
            m2.put("columnChName","负责产品");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_ka_code");
            m2.put("columnValue",kaTerritoryKaCode);
            m2.put("columnChName","KA负责人岗位");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_ka_cwid");
            m2.put("columnValue",kaCwid);
            m2.put("columnChName","KA负责人Cwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_ka_name");
            m2.put("columnValue",kaName);
            m2.put("columnChName","KA负责人名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_tuozhan_code");
            m2.put("columnValue",kaTerritoryExpandCode);
            m2.put("columnChName","KA拓展经理岗位");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_tuozhan_cwid");
            m2.put("columnValue",kaExCwid);
            m2.put("columnChName","拓展主任CWID");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_tuozhan_name");
            m2.put("columnValue",kaExName);
            m2.put("columnChName","拓展主任姓名");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_tuozhan_countby");
            m2.put("columnValue",territoryTuozhanCountby);
            m2.put("columnChName","计量方式");
            l.add(m2);

//            m2 = new HashMap();
//            m2.put("columnEnName","hco_dadan_ornot");
//            m2.put("columnValue","是");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","ka_target_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);
//
//            m2 = new HashMap();
//            m2.put("columnEnName","ka_visit_or_not");
//            m2.put("columnValue","是");
//            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_chainstore_hq");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_hospital_add_da
            CuspostQuarterChainstoreHqAddDa info = new CuspostQuarterChainstoreHqAddDa();
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setApplyCode(applyCode);
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
            info.setCrmCode(crmCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setPrintListConditionCode(printListConditionCode);
            info.setPrintListConditionName(printListConditionName);
            info.setDsmCode(dsmCode);
            info.setDsmCwid(dsmCwid);
            info.setDsmName(dsmName);
            info.setKaTerritoryKaCode(kaTerritoryKaCode);
            info.setKaTerritoryKaCwid(kaCwid);
            info.setKaTerritoryKaName(kaName);
            info.setKaTerritoryExpandCode(kaTerritoryExpandCode);
            info.setKaTerritoryExpandCwid(kaExCwid);
            info.setKaTerritoryExpandName(kaExName);
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            //20230529 START
            info.setTelephone(telephone);
            info.setKaUpStreamLeChoId(kaUpStreamLeChoId);
            info.setKaUpStreamLeName(kaUpStreamLeName);
            info.setTerritoryProducts(territoryProducts);
            info.setTerritoryTuozhanCountby(territoryTuozhanCountby);
            info.setGongcangHcoId(gongcangHcoId);
            info.setGongcangName(gongcangName);
            info.setGongcangGrade(gongcangGrade);
            //20230529 END
            int insertCount = cuspostQuarterChainstoreHqAddDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_dsm*/
            CuspostQuarterChainstoreHqAddDsm info1 = new CuspostQuarterChainstoreHqAddDsm();
            UpdateWrapper<CuspostQuarterChainstoreHqAddDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("customerCode", customerCode); //20230117
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.set("approver", "");//20230529 20230621
            updateWrapper1.eq("applyCode", applyCode);
            int insertCount1 = cuspostQuarterChainstoreHqAddDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_chainstore_hq_add_assistant*/
            CuspostQuarterChainstoreHqAddAssistant info2 = new CuspostQuarterChainstoreHqAddAssistant();
            UpdateWrapper<CuspostQuarterChainstoreHqAddAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("customerCode", customerCode); //20230117
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.set("approver", "");//20230529 20230621
            updateWrapper2.eq("applyCode", applyCode);
            int insertCount2 = cuspostQuarterChainstoreHqAddAssistantMapper.update(info2, updateWrapper2);

            // 更新到正式库
            if ("1".equals(approveTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int delFlag = customerPostMapper.deleteHcoChainstoreHqByCusName(model);// 20230414 主数据中dsm无值，也可以进行新增
                int f = customerPostMapper.insertHcoChainstoreHqFromDa(model);
            }

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateChainstoreHqAddDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateChainstoreHqAddAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionChainstoreHqAdd(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            logger.info(e.getMessage());
            logger.info(e.getStackTrace());
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * DA上传医院变更删除审批结果
     */
    @ApiOperation(value = "DA上传医院变更删除审批结果", notes = "DA上传医院变更删除审批结果")
    @RequestMapping("/batchAddHospitalChangeDeletionQuarterFromDa")
    @Transactional
    public Wrapper batchAddHospitalChangeDeletionQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_HOSPITAL_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = hospitalChangeDeletionQuarterFromDaDataBatch("cuspost_quarter_hospital_change_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String hospitalChangeDeletionQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            /**校验 架构城市关系*/
            String relation2 = customerPostMapper.updateCheckFromStructureCity(
                    tableEnNameTem, nowYM, manageMonth, UserConstant.CUSTOMER_TYPE_HOSPITAL, fileId, UserConstant.APPLY_TYPE_CODE2, null);
            if (relation2 != null) {
                String messageContent = " 客户 【" + relation2 + "】的架构城市关系不正确，请确认！";
                errorMessageList.add(messageContent);
            }

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterHospitalChangeDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_dsm_code");
//                m2.put("columnValue",stringStringMap.get("dsmCode"));
//                m2.put("columnChName","DSM岗位代码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","hp_territory_mr_code");
//                m2.put("columnValue",stringStringMap.get("repCode"));
//                m2.put("columnChName","Rep岗位代码");
//                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_hospital");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//
//            }
            //20240220 END


            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaChangeByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadHospitalChangeDeletionFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateHospitalDsmFromDaChange(fileId);
                customerPostMapper.updateHospitalAssiFromDaChange(fileId);

                // 插入新数据（cuspost_quarter_hospital_add_da）
                customerPostMapper.uploadHospitalChangeDeletionFromDaInsert(fileId, userCode);

                // 插入新数据，判断是同意还是驳回（hub_hco_hospital）
                customerPostMapper.uploadHubHcoHospitalChangeDeletionFromDaUpdate(fileId, userCode);

                //20230518 删除核心数据，同意且调整类型是删除的场合
                customerPostMapper.uploadHubHcoHospitalChangeDeletionFromDaDelete(fileId);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateHospitalChangeDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateHospitalChangeAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionHospitalChange(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * DA上传零售终端变更删除审批结果
     */
    @ApiOperation(value = "DA上传零售终端变更删除审批结果", notes = "DA上传零售终端变更删除审批结果")
    @RequestMapping("/batchAddRetailChangeDeletionQuarterFromDa")
    @Transactional
    public Wrapper batchAddRetailChangeDeletionQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_RETAIL_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = retailChangeDeletionQuarterFromDaDataBatch("cuspost_quarter_retail_change_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                    //20231113 删除零售未分配关店数据
                    customerPostMapper.deleteRetailNotAssignedShutupShop(manageYear, manageQuarter);
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String retailChangeDeletionQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterRetailChangeDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_longitude");
//                m2.put("columnValue",stringStringMap.get("iongitude"));
//                m2.put("columnChName","经度");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","rt_latitude");
//                m2.put("columnValue",stringStringMap.get("iatitude"));
//                m2.put("columnChName","维度");
//                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_retail");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//            }
            //20240220 END

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaChangeByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadRetailChangeDeletionFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateRetailDsmFromDaChange(fileId);
                customerPostMapper.updateRetailAssiFromDaChange(fileId);

                // 插入新数据（cuspost_quarter_retail_add_da）
                customerPostMapper.uploadRetailChangeDeletionFromDaInsert(fileId, userCode);

                // 插入新数据，判断是同意还是驳回（hub_hco_retail）
                customerPostMapper.uploadHubHcoRetailChangeDeletionFromDaUpdate(fileId, userCode);

                // 20230406 Hazard 区域验证数据源逻辑调整
//                customerPostMapper.insertThirdPartyAreaChange(fileId, userCode);// TODO 20230424 区域验证数据源逻辑调整

                //20230518  删除核心数据，同意且调整类型是删除的场合
                customerPostMapper.uploadHubHcoRetailChangeDeletionFromDaDelete(fileId);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateRetailChangeDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRetailChangeAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionRetailChange(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * DA上传商务变更删除审批结果
     */
    @ApiOperation(value = "DA上传商务变更删除审批结果", notes = "DA上传商务变更删除审批结果")
    @RequestMapping("/batchAddDistributorChangeDeletionQuarterFromDa")
    @Transactional
    public Wrapper batchAddDistributorChangeDeletionQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_DISTRIBUTOR_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = distributorChangeDeletionQuarterFromDaDataBatch("cuspost_quarter_distributor_change_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String distributorChangeDeletionQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterDistributorChangeDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","cmc_territory_dsm_code");
//                m2.put("columnValue",stringStringMap.get("dsmCode"));
//                m2.put("columnChName","DSM岗位代码");
//                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_distributor");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//            }
            //20240220 END

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaChangeByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadDistributorChangeDeletionFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateDistributorDsmFromDaChange(fileId);
                customerPostMapper.updateDistributorAssiFromDaChange(fileId);

                // 插入新数据（cuspost_quarter_distributor_add_da）
                customerPostMapper.uploadDistributorChangeDeletionFromDaInsert(fileId, userCode);

                // 插入新数据，判断是同意还是驳回（hub_hco_distributor）
                customerPostMapper.uploadHubHcoDistributorChangeDeletionFromDaUpdate(fileId, userCode);

                //20230518  删除核心数据，同意且调整类型是删除的场合
                customerPostMapper.uploadHubHcoDistributorChangeDeletionFromDaDelete(fileId);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateDistributorChangeDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateDistributorChangeAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionDistributorChange(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * DA上传连锁总部变更删除审批结果
     */
    @ApiOperation(value = "DA上传连锁总部变更删除审批结果", notes = "DA上传连锁总部变更删除审批结果")
    @RequestMapping("/batchAddChainstoreHqChangeDeletionQuarterFromDa")
    @Transactional
    public Wrapper batchAddChainstoreHqChangeDeletionQuarterFromDa(HttpServletRequest request) {
        try {
            // 取得画面参数
            logger.info("保存上传文件");
            int manageYear = Integer.parseInt(request.getParameter("manageYear"));
            String manageQuarter = request.getParameter("manageQuarter");

            //创建季度调整任务按钮 做校验同一年度，季度不让再插入
            CuspostQuarterAdjustInfo cuspostInfo = cuspostQuarterAdjustInfoMapper.selectOne(
                    new QueryWrapper<CuspostQuarterAdjustInfo>()
                            .eq("manageYear", manageYear)
                            .eq("manageQuarter", manageQuarter)
            );
            if (StringUtils.isEmpty(cuspostInfo)) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "季度客岗任务没有创建");
            }

            MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
            String userCode = loginUser.getUserCode();

            Map<String, String> filenames = customerPostExcelUploadUtils.uploadForSaveFile(request, cusPostFileUploadPath);
            if (filenames == null) {
                return Wrapper.info(ResponseConstant.DATA_CHECK_ERROR_CODE, "文件保存错误，请联系系统管理员！");
            }
            String oldFileName = filenames.get("oldFileName");
            String newFIleName = filenames.get("newFileName");

            // 读取头配置
            List<UploadItemExplainModel> uploadItemExplainModelList = masterCommonMapper.getMasterExplainModelList(UserConstant.QUARTER_DA_CHAINSTORE_HQ_CHANGE);
            List<UploadItemExplainModel> uploadItemExplainModels = uploadItemExplainModelList.stream().filter(
                    uploadItemExplainModel -> "1".equals(uploadItemExplainModel.getIsUploadItem())).collect(Collectors.toList());

            // 生成版本号
            String fileId = commonUtils.createUUID();

            CuspostQuarterDataUploadInfo masterUploadFile = new CuspostQuarterDataUploadInfo();
            masterUploadFile.setFileID(fileId);
            masterUploadFile.setUploadFileName(oldFileName);
            masterUploadFile.setNewFileName(newFIleName);
            masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_READING);
            cuspostQuarterDataUploadInfoMapper.insert(masterUploadFile);

            // 检查上传文件基本格式
            String errorMessage = customerPostExcelUploadUtils.excelUploadForTemplateCheck(uploadItemExplainModels, newFIleName);

            if (StringUtils.isEmpty(errorMessage)) {

                // 上传文件处理
                String errorFileName = chainstoreHqChangeDeletionQuarterFromDaDataBatch("cuspost_quarter_chainstore_hq_change_da", uploadItemExplainModels,
                        fileId, newFIleName, userCode, manageYear, manageQuarter);

                if ("".equals(errorFileName)) {
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_OVER);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                    //没有错误
                } else if ("-1".equals(errorFileName)) {
                    masterUploadFile.setErrorMessage("系统错误，请联系系统管理员！");
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                } else {
                    masterUploadFile.setErrorMessage("详细参照，失败详细文件！");
                    masterUploadFile.setErrorFileName(errorFileName);
                    masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                    cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
                }
            } else {
                masterUploadFile.setErrorMessage(errorMessage);
                masterUploadFile.setUploadState(UserConstant.FILE_UPLOAD_STATE_ERROR);
                cuspostQuarterDataUploadInfoMapper.updateById(masterUploadFile);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        logger.info("上传完成！");
        return Wrapper.success();
    }

    /**
     * 数据批量新增更新处理
     */
    @Transactional
    public String chainstoreHqChangeDeletionQuarterFromDaDataBatch(String tableEnName, List<UploadItemExplainModel> uploadItemExplainModels, String fileId, String fileName, String userCode, int manageYear, String manageQuarter) {
        String errorFileName = "";
        String tableEnNameTem = UserConstant.UPLOAD_TABLE_PREFIX + tableEnName;
        try {
            String nowYM = commonUtils.getTodayYM2();
            //生成下一季度第一个月字段
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            // 读取数据到临时表
            List<String> errorMessageList = customerPostExcelUploadUtils.excelUploadUtils(
                    tableEnName, uploadItemExplainModels, fileId, fileName, 0, UserConstant.LEFT_CHECK_TYPE_NOTHING, manageMonth);

            //20230628 check驳回的场合，审批意见为空的数据
            String approvalOpinionIsNull = customerPostMapper.checkDaApprovalOpinionIsNull(tableEnNameTem, fileId);
            if (approvalOpinionIsNull != null) {
                String messageContent = " 申请编码 【" + approvalOpinionIsNull + "】驳回的审批意见为空，请确认！";
                errorMessageList.add(messageContent);
            }

            //20240220 START
            //获取up_的数据
//            List<Map<String, String>> upDaList = customerPostMapper.queryUpCuspostQuarterChainstoreHqChangeDaById(fileId);
//            for (Map<String, String> stringStringMap : upDaList) {
//                List<Map<String, Object>> l = new ArrayList<>();
//                Map<String, Object> m = new HashMap();
//                Map<String, Object> m2 = new HashMap();
//
//                m2 = new HashMap();
//                m2.put("columnEnName","manageMonth");
//                m2.put("columnValue",BigDecimal.valueOf(manageMonth));
//                m2.put("columnChName","年月");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_id");
//                m2.put("columnValue",stringStringMap.get("customerCode"));
//                m2.put("columnChName","客户编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_name");
//                m2.put("columnValue",stringStringMap.get("customerName"));
//                m2.put("columnChName","客户名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","province");
//                m2.put("columnValue",stringStringMap.get("province"));
//                m2.put("columnChName","省份");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","city");
//                m2.put("columnValue",stringStringMap.get("city"));
//                m2.put("columnChName","城市");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_up_stream_le_cho_id");
//                m2.put("columnValue",stringStringMap.get("kaUpStreamLeChoId"));
//                m2.put("columnChName","归属上级编码");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_up_stream_le_name");
//                m2.put("columnValue",stringStringMap.get("kaUpStreamLeName"));
//                m2.put("columnChName","归属上级名称");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_county");
//                m2.put("columnValue",stringStringMap.get("county"));
//                m2.put("columnChName","区县");
//                l.add(m2);
//
//                m2 = new HashMap();
//                m2.put("columnEnName","ka_address");
//                m2.put("columnValue",stringStringMap.get("address"));
//                m2.put("columnChName","地址");
//                l.add(m2);
//
//                m.put("type", "1");
//                m.put("tableEnName", "hco_chainstore_hq");
//                m.put("column", l);
//
//                Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);
//                if (!stringObjectMap.isEmpty() && "1".equals(stringStringMap.get("approveTypeCode"))) {
//                    errorMessageList.add(" 客户编码 【" + stringStringMap.get("customerCode") + "】 : " + stringObjectMap.get("message").toString());
//                }
//            }
            //20240220 END

            // 存在读取文件错误的场合生成错误文件
            if (errorMessageList != null && errorMessageList.size() > 0) {
                errorFileName = commonUtils.createUUID() + ".csv";
                CsvWriter csvWriter = new CsvWriter(cusPostErrorfilePath + errorFileName, ',', Charset.forName("GBK"));
                String[] csvHeaders = {"错误信息"};
                csvWriter.writeRecord(csvHeaders);
                for (int i = 0; i < errorMessageList.size(); i++) {

                    String[] csvContent = {
                            errorMessageList.get(i)
                    };
                    csvWriter.writeRecord(csvContent);
                }
                csvWriter.close();

            } else {
                //删除已经上传过的数据
                customerPostMapper.deleteUploadDaChangeByExist(tableEnName, fileId, manageYear, manageQuarter);
                //更新年度，季度,年月，region，dsmCwid，dsmName，repCwid，repName，applyStateCode，addType
                customerPostMapper.uploadChainstoreHqChangeDeletionFromDa(fileId, manageYear, manageQuarter, manageMonth, nowYM);

                // 更新上传数据 上传的已经结束，更新没有意义
                // 更新dsm/助理审批意见，状态
                customerPostMapper.updateChainstoreHqDsmFromDaChange(fileId);
                customerPostMapper.updateChainstoreHqAssiFromDaChange(fileId);

                // 插入新数据（cuspost_quarter_chainstore_hq_add_da）
                customerPostMapper.uploadChainstoreHqChangeDeletionFromDaInsert(fileId, userCode);

                // 插入新数据，判断是同意还是驳回（hub_hco_chainstore_hq）
                customerPostMapper.uploadHubHcoChainstoreHqChangeDeletionFromDaUpdate(fileId, userCode);


                //20230518  删除核心数据，同意且调整类型是删除的场合
                customerPostMapper.uploadHubHcoChainstoreHqChangeDeletionFromDaDelete(fileId);

                //20230519 D&A审批更新后，一览状态逻辑判断
                customerPostMapper.updateQuarterApplyStateChainstoreHqChangeDsm(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateChainstoreHqChangeAss(manageYear, manageQuarter);
                customerPostMapper.updateQuarterApplyStateRegionChainstoreHqChange(manageYear, manageQuarter);
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            errorFileName = "-1";
        } finally {
            // 删除临时表数据
            customerPostMapper.deleteTemTableData(fileId, tableEnNameTem);
        }
        return errorFileName;
    }

    /**
     * D&A医院变更删除审批
     */
    @ApiOperation(value = "D&A医院变更删除审批", notes = "D&A医院变更删除审批")
    @RequestMapping(value = "/approveHospitalChangeDeletionQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveHospitalChangeDeletionQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); //客户编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
//            String dsmCode = object.getString("dsmCode"); //DSM岗位代码
//            String repCode = object.getString("repCode"); //REP岗位代码
            String dsmCode = StringUtils.isEmpty(object.getString("dsmCode")) ? "" : object.getString("dsmCode"); //DSM岗位代码
            String repCode = StringUtils.isEmpty(object.getString("repCode")) ? "" : object.getString("repCode"); //REP岗位代码
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String adjustTypeCode = object.getString("adjustTypeCode"); //如果等于1为关店数据，需要删除
            String applyCode = object.getString("applyCode"); // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();

            /**校验 架构城市关系*/
            int countFromStructureCity = customerPostMapper.queryCountFromStructureCity(nowYM, repCode, city);
            if (countFromStructureCity < 1) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "架构城市关系错误", "架构城市关系不正确！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //获取repName,repCwid
            Map<String, String> repMap = customerPostMapper.getDataNameByDataCode(nowYM, repCode);
            String repName = null;
            String repCwid = null;
            if (!StringUtils.isEmpty(repMap)) {
                repName = repMap.get("userName");
                repCwid = repMap.get("cwid");
            } else {
                //架构错误
            }

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_dsm_code");
            m2.put("columnValue",dsmCode);
            m2.put("columnChName","DSM岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_dsm_cwid");
            m2.put("columnValue",dsmCwid);
            m2.put("columnChName","DSMCwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_dsm_name");
            m2.put("columnValue",dsmName);
            m2.put("columnChName","DSM名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_mr_code");
            m2.put("columnValue",repCode);
            m2.put("columnChName","Rep岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_mr_cwid");
            m2.put("columnValue",repCwid);
            m2.put("columnChName","RepCwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","hp_territory_mr_name");
            m2.put("columnValue",repName);
            m2.put("columnChName","Rep名称");
            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_hospital");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_hospital_change_da
            CuspostQuarterHospitalChangeDa info = new CuspostQuarterHospitalChangeDa();
            info.setApplyCode(applyCode);// 20230424 变更申请编码
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setDsmCode(dsmCode);
            info.setDsmCwid(dsmCwid);
            info.setDsmName(dsmName);
            info.setRepCode(repCode);
            info.setRepCwid(repCwid);
            info.setRepName(repName);
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            int insertCount = cuspostQuarterHospitalChangeDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_hospital_change_dsm*/
            CuspostQuarterHospitalChangeDsm info1 = new CuspostQuarterHospitalChangeDsm();
            UpdateWrapper<CuspostQuarterHospitalChangeDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.eq("customerCode", customerCode);
            updateWrapper1.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount1 = cuspostQuarterHospitalChangeDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_hospital_change_assistant*/
            CuspostQuarterHospitalChangeAssistant info2 = new CuspostQuarterHospitalChangeAssistant();
            UpdateWrapper<CuspostQuarterHospitalChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterHospitalChangeAssistantMapper.update(info2, updateWrapper2);


            //调整类型不是删除的 更新hub_hco_hospital
            //调整类型是删除的 删除hub_hco_hospital
            //同意且不是删除数据
            if ("1".equals(approveTypeCode) && !"1".equals(adjustTypeCode) && !"2".equals(adjustTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.updateHcoHospitalFromDa(model);
            }
            //同意且是删除数据
            if ("1".equals(approveTypeCode) && ("1".equals(adjustTypeCode) || "2".equals(adjustTypeCode))) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.deleteHcoHospitalByAdjustType(model);
            }

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateHospitalChangeDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateHospitalChangeAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionHospitalChange(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * D&A零售终端变更删除审批
     */
    @ApiOperation(value = "D&A零售终端变更删除审批", notes = "D&A零售终端变更删除审批")
    @RequestMapping(value = "/approveRetailChangeDeletionQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveRetailChangeDeletionQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); //客户编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String adjustTypeCode = object.getString("adjustTypeCode"); //如果等于1为关店数据，需要删除
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_longitude");
            m2.put("columnValue",iongitude);
            m2.put("columnChName","经度");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","rt_latitude");
            m2.put("columnValue",iatitude);
            m2.put("columnChName","维度");
            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_retail");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_retail_change_da
            CuspostQuarterRetailChangeDa info = new CuspostQuarterRetailChangeDa();
            info.setApplyCode(applyCode);// 20230424 变更申请编码
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            int insertCount = cuspostQuarterRetailChangeDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_retail_change_dsm*/
            CuspostQuarterRetailChangeDsm info1 = new CuspostQuarterRetailChangeDsm();
            UpdateWrapper<CuspostQuarterRetailChangeDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.eq("customerCode", customerCode);
            updateWrapper1.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount1 = cuspostQuarterRetailChangeDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_retail_change_assistant*/
            CuspostQuarterRetailChangeAssistant info2 = new CuspostQuarterRetailChangeAssistant();
            UpdateWrapper<CuspostQuarterRetailChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterRetailChangeAssistantMapper.update(info2, updateWrapper2);


            //调整类型不是删除的 更新hub_hco_retail
            //调整类型是删除的 删除hub_hco_retail
            //同意且不是删除数据
            if ("1".equals(approveTypeCode) && !"1".equals(adjustTypeCode) && !"2".equals(adjustTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.updateHcoRetailFromDa(model);

                // 20230406 Hazard 区域验证数据源逻辑调整
                //TODO 20230424 区域验证数据源逻辑调整
//                CuspostQuarterThirdPartyArea thirdPartyArea = new CuspostQuarterThirdPartyArea();
//                thirdPartyArea.setManageYear(BigDecimal.valueOf(manageYear));
//                thirdPartyArea.setManageQuarter(manageQuarter);
//                thirdPartyArea.setYearMonth(BigDecimal.valueOf(manageMonth));
//                thirdPartyArea.setCustomerCode(customerCode);
//                thirdPartyArea.setCustomerName(customerName);
//                thirdPartyArea.setProvince(province);
//                thirdPartyArea.setCity(city);
//                thirdPartyArea.setAddress(address);
//                thirdPartyArea.setInsertUser(loginUser.getUserCode());
//                thirdPartyArea.setInsertTime(new Date());
//                cuspostQuarterThirdPartyAreaMapper.insert(thirdPartyArea);
            }
            //同意且是删除数据
            if ("1".equals(approveTypeCode) && ("1".equals(adjustTypeCode) || "2".equals(adjustTypeCode))) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.deleteHcoRetailByAdjustType(model);
            }

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateRetailChangeDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRetailChangeAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionRetailChange(manageYear, manageQuarter);

            //20231113 删除零售未分配关店数据
            customerPostMapper.deleteRetailNotAssignedShutupShop(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * D&A商务变更删除审批
     */
    @ApiOperation(value = "D&A商务变更删除审批", notes = "D&A商务变更删除审批")
    @RequestMapping(value = "/approveDistributorChangeDeletionQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveDistributorChangeDeletionQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); //客户编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
//            String dsmCode = object.getString("dsmCode"); //DSM岗位代码
            String dsmCode = StringUtils.isEmpty(object.getString("dsmCode")) ? "" : object.getString("dsmCode"); //DSM岗位代码
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String adjustTypeCode = object.getString("adjustTypeCode"); //如果等于1为关店数据，需要删除
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(commonUtils.getTodayYM2(), dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_territory_dsm_code");
            m2.put("columnValue",dsmCode);
            m2.put("columnChName","DSM岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","cmc_territory_dsm_cwid");
            m2.put("columnValue",dsmCwid);
            m2.put("columnChName","DSMCwid");
            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_distributor");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_distributor_change_da
            CuspostQuarterDistributorChangeDa info = new CuspostQuarterDistributorChangeDa();
            info.setApplyCode(applyCode);// 20230424 变更申请编码
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setDsmCode(dsmCode);
            info.setDsmCwid(dsmCwid);
            info.setDsmName(dsmName);
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            int insertCount = cuspostQuarterDistributorChangeDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_distributor_change_dsm*/
            CuspostQuarterDistributorChangeDsm info1 = new CuspostQuarterDistributorChangeDsm();
            UpdateWrapper<CuspostQuarterDistributorChangeDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.eq("customerCode", customerCode);
            updateWrapper1.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount1 = cuspostQuarterDistributorChangeDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_distributor_change_assistant*/
            CuspostQuarterDistributorChangeAssistant info2 = new CuspostQuarterDistributorChangeAssistant();
            UpdateWrapper<CuspostQuarterDistributorChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterDistributorChangeAssistantMapper.update(info2, updateWrapper2);


            //调整类型不是删除的 更新hub_hco_distributor
            //调整类型是删除的 删除hub_hco_distributor
            //同意且不是删除数据
            if ("1".equals(approveTypeCode) && !"1".equals(adjustTypeCode) && !"2".equals(adjustTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.updateHcoDistributorFromDa(model);
            }
            //同意且是删除数据
            if ("1".equals(approveTypeCode) && ("1".equals(adjustTypeCode) || "2".equals(adjustTypeCode))) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.deleteHcoDistributorByAdjustType(model);
            }

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateDistributorChangeDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateDistributorChangeAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionDistributorChange(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    /**
     * D&A连锁总部变更删除审批
     */
    @ApiOperation(value = "D&A连锁总部变更删除审批", notes = "D&A连锁总部变更删除审批")
    @RequestMapping(value = "/approveChainstoreHqChangeDeletionQuarterByDa", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = RequestMethod.POST)
    @Transactional
    public Wrapper approveChainstoreHqChangeDeletionQuarterByDa(@RequestBody String json) {
        // 返回的数据
        Map<String, Object> resultMap = new HashMap<>();
        MasterUserInfo loginUser = CurrentUserUtils.getCurrentLoginUser();
        try {
            // 画面参数取得
            JSONObject object = JSON.parseObject(json);
            int manageYear = object.getInteger("manageYear"); // 年度
            String manageQuarter = object.getString("manageQuarter"); // 季度
            String customerCode = object.getString("customerCode"); //客户编码
            String customerName = object.getString("verifyCustomerName"); //验证后客户名称
            String province = object.getString("province"); //省份
            String city = object.getString("city"); //城市
            String county = object.getString("county"); //区县
            String address = object.getString("address"); //地址
            String iongitude = object.getString("iongitude"); //经度
            String iatitude = object.getString("iatitude"); //纬度
//            String dsmCode = object.getString("dsmCode"); //DSM岗位代码
            String dsmCode = StringUtils.isEmpty(object.getString("dsmCode")) ? "" : object.getString("dsmCode"); //DSM岗位代码
            String approveTypeCode = object.getString("approveTypeCode"); //审批结果 （1同意，2驳回）
            String approvalOpinion = object.getString("approvalOpinion"); //审批意见
            String adjustTypeCode = object.getString("adjustTypeCode"); //如果等于1为关店数据，需要删除
            String kaUpStreamLeChoId = object.getString("kaUpStreamLeChoId");           // 归属上级编码 20230301
            String kaUpStreamLeName = object.getString("kaUpStreamLeName");             // 归属上级名称 20230301
            String applyCode = object.getString("applyCode");                           // 20230424 变更申请编码

            // 必须检查
            if (StringUtils.isEmpty(applyCode)// 20230424 变更申请编码
            ) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "参数错误", "输出参数不可以为空！");
            }

            String nowYM = commonUtils.getTodayYM2();
            //获取dsmName,dsmCwid
            Map<String, String> dsmMap = customerPostMapper.getDataNameByDataCode(nowYM, dsmCode);
            String dsmName = null;
            String dsmCwid = null;
            if (!StringUtils.isEmpty(dsmMap)) {
                dsmName = dsmMap.get("userName");
                dsmCwid = dsmMap.get("cwid");
            } else {
                //架构错误
            }

            //manageMonth
            int manageMonth = this.creatYearMonth(manageYear, manageQuarter);

            //applyStateCode
            String applyStateCode = null;
            if ("1".equals(approveTypeCode)) {
                applyStateCode = UserConstant.APPLY_STATE_CODE_6;
            } else {
                applyStateCode = UserConstant.APPLY_STATE_CODE_7;
            }

            //20240220 START
            List<Map<String, Object>> l = new ArrayList<>();
            Map<String, Object> m = new HashMap();
            Map<String, Object> m2 = new HashMap();

            m2 = new HashMap();
            m2.put("columnEnName","manageMonth");
            m2.put("columnValue",BigDecimal.valueOf(manageMonth));
            m2.put("columnChName","年月");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_id");
            m2.put("columnValue",customerCode);
            m2.put("columnChName","客户编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_name");
            m2.put("columnValue",customerName);
            m2.put("columnChName","客户名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","province");
            m2.put("columnValue",province);
            m2.put("columnChName","省份");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","city");
            m2.put("columnValue",city);
            m2.put("columnChName","城市");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_up_stream_le_cho_id");
            m2.put("columnValue",kaUpStreamLeChoId);
            m2.put("columnChName","归属上级编码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_up_stream_le_name");
            m2.put("columnValue",kaUpStreamLeName);
            m2.put("columnChName","归属上级名称");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_county");
            m2.put("columnValue",county);
            m2.put("columnChName","区县");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_address");
            m2.put("columnValue",address);
            m2.put("columnChName","地址");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_dsm_code");
            m2.put("columnValue",dsmCode);
            m2.put("columnChName","DSM岗位代码");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_dsm_cwid");
            m2.put("columnValue",dsmCwid);
            m2.put("columnChName","DSMCwid");
            l.add(m2);

            m2 = new HashMap();
            m2.put("columnEnName","ka_territory_dsm_name");
            m2.put("columnValue",dsmName);
            m2.put("columnChName","DSM岗位名称");
            l.add(m2);

            m.put("type", "1");
            m.put("tableEnName", "hco_chainstore_hq");
            m.put("column", l);

            Map<String, Object> stringObjectMap = cuspostCommonService.dynamicColumnCheck(m);

            if (!stringObjectMap.isEmpty() && "1".equals(approveTypeCode)) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "校验错误", stringObjectMap.get("message").toString());
            }
            //20240220 END

            // 存入Da数据表 cuspost_quarter_chainstore_hq_change_da
            CuspostQuarterChainstoreHqChangeDa info = new CuspostQuarterChainstoreHqChangeDa();
            info.setApplyCode(applyCode);// 20230424 变更申请编码
            info.setManageYear(BigDecimal.valueOf(manageYear));
            info.setManageQuarter(manageQuarter);
            info.setYearMonth(BigDecimal.valueOf(manageMonth));
            info.setCustomerName(customerName);
            info.setCustomerCode(customerCode);
            info.setProvince(province);
            info.setCity(city);
            info.setCounty(county);
            info.setAddress(address);
            info.setIongitude(iongitude);
            info.setIatitude(iatitude);
            info.setDsmCode(dsmCode);
            info.setDsmCwid(dsmCwid);
            info.setDsmName(dsmName);
            info.setKaUpStreamLeChoId(kaUpStreamLeChoId); // 归属上级编码 20230301
            info.setKaUpStreamLeName(kaUpStreamLeName); // 归属上级编码 20230301
            info.setApplyStateCode(applyStateCode);
            info.setApproveTypeCode(approveTypeCode);
            info.setApprovalOpinion(approvalOpinion);
            info.setAddType("1");//单条
            int insertCount = cuspostQuarterChainstoreHqChangeDaMapper.insert(info);

            if (insertCount <= 0) {
                return Wrapper.infoTitle(ResponseConstant.ERROR_CODE, "执行错误", "数据更新失败！");
            }

            // 更新申请状态（明细）
            /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_dsm*/
            CuspostQuarterChainstoreHqChangeDsm info1 = new CuspostQuarterChainstoreHqChangeDsm();
            UpdateWrapper<CuspostQuarterChainstoreHqChangeDsm> updateWrapper1 = new UpdateWrapper<>();
            updateWrapper1.set("applyStateCode", applyStateCode);
            updateWrapper1.set("approvalOpinion", approvalOpinion);
//            updateWrapper1.eq("customerCode", customerCode);
            updateWrapper1.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount1 = cuspostQuarterChainstoreHqChangeDsmMapper.update(info1, updateWrapper1);

            /**更新申请编码状态 cuspost_quarter_chainstore_hq_change_assistant*/
            CuspostQuarterChainstoreHqChangeAssistant info2 = new CuspostQuarterChainstoreHqChangeAssistant();
            UpdateWrapper<CuspostQuarterChainstoreHqChangeAssistant> updateWrapper2 = new UpdateWrapper<>();
            updateWrapper2.set("applyStateCode", applyStateCode);
            updateWrapper2.set("approvalOpinion", approvalOpinion);
//            updateWrapper2.eq("customerCode", customerCode);
            updateWrapper2.eq("applyCode", applyCode);// 20230424 变更申请编码
            int insertCount2 = cuspostQuarterChainstoreHqChangeAssistantMapper.update(info2, updateWrapper2);


            //调整类型不是删除的 更新hub_hco_chainstore_hq
            //调整类型是删除的 删除hub_hco_chainstore_hq
            //同意且不是删除数据
            if ("1".equals(approveTypeCode) && !"1".equals(adjustTypeCode) && !"2".equals(adjustTypeCode)) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.updateHcoChainstoreHqFromDa(model);
            }
            //同意且是删除数据
            if ("1".equals(approveTypeCode) && ("1".equals(adjustTypeCode) || "2".equals(adjustTypeCode))) {
                CustomerPostModel model = new CustomerPostModel();
                BeanUtils.copyProperties(info, model);
                model.setInsertUser(loginUser.getUserCode());
                int f2 = customerPostMapper.deleteHcoChainstoreHqByAdjustType(model);
            }

            //20230519 D&A审批更新后，一览状态逻辑判断
            customerPostMapper.updateQuarterApplyStateChainstoreHqChangeDsm(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateChainstoreHqChangeAss(manageYear, manageQuarter);
            customerPostMapper.updateQuarterApplyStateRegionChainstoreHqChange(manageYear, manageQuarter);
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e);
            return Wrapper.error();
        }
        return Wrapper.success(resultMap);
    }

    //endregion

    private int creatYearMonth(int manageYear, String manageQuarter) {
        int manageMonth = 0;
        if ("Q1".equals(manageQuarter)) {
            manageMonth = Integer.parseInt(manageYear + "01");
        }
        if ("Q2".equals(manageQuarter)) {
            manageMonth = Integer.parseInt(manageYear + "04");
        }
        if ("Q3".equals(manageQuarter)) {
            manageMonth = Integer.parseInt(manageYear + "07");
        }
        if ("Q4".equals(manageQuarter)) {
            manageMonth = Integer.parseInt(manageYear + "10");
        }
        return manageMonth;
    }

    //创建applyCode申请编码
//    private String getApplyCode(int manageYear, String manageQuarter) {
//        //获取applyCode
//        CuspostQuarterApplyCodeInfo applyCodeQuarterInfo = cuspostQuarterApplyCodeInfoMapper.selectOne(
//                new QueryWrapper<CuspostQuarterApplyCodeInfo>()
//                        .eq("manageYear", manageYear)
//                        .eq("manageQuarter", manageQuarter)
//        );
//
//        if (StringUtils.isEmpty(applyCodeQuarterInfo)) {
//            return null;
//        }
//
//        //编辑申请编码
//        int applyCodeInt = applyCodeQuarterInfo.getApplyCode() + 1;
//        String applyCodeStr = "JDSQ" + manageYear + manageQuarter + String.format("%06d", applyCodeInt);
//
//        //更新applyCode
//        CuspostQuarterApplyCodeInfo updateApplyCodeQuarterInfo = new CuspostQuarterApplyCodeInfo();
//        UpdateWrapper<CuspostQuarterApplyCodeInfo> updateWrapper = new UpdateWrapper<>();
//        updateWrapper.set("applyCode", applyCodeInt);
//        updateWrapper.eq("autoKey", applyCodeQuarterInfo.getAutoKey());
//        cuspostQuarterApplyCodeInfoMapper.update(updateApplyCodeQuarterInfo, updateWrapper);
//
//        return applyCodeStr;
//    }

    //创建applyCode申请编码
//    private int getApplyCodeBatch(int manageYear, String manageQuarter, int count) {
//        //获取applyCode
//        CuspostQuarterApplyCodeInfo applyCodeQuarterInfo = cuspostQuarterApplyCodeInfoMapper.selectOne(
//                new QueryWrapper<CuspostQuarterApplyCodeInfo>()
//                        .eq("manageYear", manageYear)
//                        .eq("manageQuarter", manageQuarter)
//        );
//
//        if (StringUtils.isEmpty(applyCodeQuarterInfo)) {
//            return -1;
//        }
//
//        //编辑申请编码
//        int applyCodeInt = applyCodeQuarterInfo.getApplyCode();
//        int applyCodeForSet = applyCodeQuarterInfo.getApplyCode() + count;
//
//        //更新applyCode
//        CuspostQuarterApplyCodeInfo updateApplyCodeQuarterInfo = new CuspostQuarterApplyCodeInfo();
//        UpdateWrapper<CuspostQuarterApplyCodeInfo> updateWrapper = new UpdateWrapper<>();
//        updateWrapper.set("applyCode", applyCodeForSet);
//        updateWrapper.eq("autoKey", applyCodeQuarterInfo.getAutoKey());
//        cuspostQuarterApplyCodeInfoMapper.update(updateApplyCodeQuarterInfo, updateWrapper);
//
//        return applyCodeInt;
//    }

//    private List<CustomerPostModel> getLvlCodeDsmAssistant(String nowYM, String postCode, String userCode) {
//        List<CustomerPostModel> cList = new ArrayList<>();
//        if (UserConstant.POST_CODE1.equals(postCode)) { //地区经理
//            cList = customerPostMapper.queryDsmLevelCode(nowYM, userCode);
//        } else if (UserConstant.POST_CODE2.equals(postCode)) { //大区助理
//            cList = customerPostMapper.queryAssistantLevelCode(nowYM, userCode);
//        } else {
//
//        }
//        return cList;
//    }

}
