package com.shj.pms.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.mapper.EntityWrapper;
import com.baomidou.mybatisplus.plugins.Page;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.shj.bid.domain.ElecContractAttach;
import com.shj.bid.domain.pmsvo.BidPmsBudgetDtlVo;
import com.shj.bid.domain.pmsvo.BidPmsPriceVo;
import com.shj.bid.domain.request.BidPmsBudgetDtlRequestParam;
import com.shj.bid.domain.request.BidPmsPriceRequestParam;
import com.shj.bid.domain.request.BidPmsSkuCodeVo;
import com.shj.bid.domain.vo.BidContractCommonVo;
import com.shj.bid.feign.BidPmsFeign;
import com.shj.bid.feign.BizopContractFeign;
import com.shj.bid.feign.ElecContractAttachFeign;
import com.shj.bpm.domain.request.FlowCheckRequest;
import com.shj.bpm.domain.request.ValidateFlowIsExistRequest;
import com.shj.bpm.domain.response.CurrentNodeResponse;
import com.shj.bpm.feign.BpmFeign;
import com.shj.caas.domain.sms.Sms;
import com.shj.caas.feign.code.CodeRuleFeign;
import com.shj.caas.feign.dictionary.DataDictionaryServiceClient;
import com.shj.caas.feign.sms.SmsClient;
import com.shj.cms.domain.request.CostControlCalcRequest;
import com.shj.cms.domain.response.ControlConfigVoResponse;
import com.shj.cms.feign.CostControlConfigFeign;
import com.shj.common.constants.SPUConstant;
import com.shj.common.constants.SpuUnitType;
import com.shj.common.dict.BidElectronicContractType;
import com.shj.common.dict.DictionaryConstants;
import com.shj.common.dict.IsDeletedStatus;
import com.shj.common.dict.PmsCheckReportStatus;
import com.shj.common.dict.PmsMaterialStatus;
import com.shj.common.dict.PmsMaterialTypeStatus;
import com.shj.common.dict.PmsOrderStatus;
import com.shj.common.dict.PmsOrderType;
import com.shj.common.dict.PmsPickingStatus;
import com.shj.common.dict.PmsPickingType;
import com.shj.common.dict.PmsTaskType;
import com.shj.common.dict.ProjectTaskStatus;
import com.shj.common.dict.SpuMaterialTypeConstants;
import com.shj.common.domain.OrderSysOperateRequest;
import com.shj.common.exception.BusinessException;
import com.shj.common.exception.InvalidParameterException;
import com.shj.common.exception.SystemException;
import com.shj.common.profile.UnifiedProfile;
import com.shj.common.service.impl.BaseEntityServiceImpl;
import com.shj.common.util.BeanUtils;
import com.shj.common.util.DateUtil;
import com.shj.common.util.HttpClientUtils;
import com.shj.common.util.UtilTool;
import com.shj.common.util.WorkNumberUtil;
import com.shj.crm.domain.bizop.BaseBizOp;
import com.shj.crm.domain.bizop.response.vo.BaseBizOpDetailResponseVoForPm;
import com.shj.crm.domain.customer.BuildingInfo;
import com.shj.crm.domain.customer.HouseInfo;
import com.shj.crm.domain.region.Region;
import com.shj.crm.feign.bizop.BaseBizOpFeign;
import com.shj.crm.feign.customer.BuildingInfoFeign;
import com.shj.crm.feign.customer.HouseInfoServiceClient;
import com.shj.crm.feign.region.RegionFeign;
import com.shj.fms.feign.receiptApply.ReceiptApplyClient;
import com.shj.mpms.domain.Inventory;
import com.shj.mpms.domain.PurchasePrice;
import com.shj.mpms.domain.SpuUnit;
import com.shj.mpms.domain.StoreLocation;
import com.shj.mpms.domain.category.Category;
import com.shj.mpms.domain.vo.SkuRelationDataVo;
import com.shj.mpms.feign.InventoryFeign;
import com.shj.mpms.feign.PurchasePriceFeign;
import com.shj.mpms.feign.SkuFeign;
import com.shj.mpms.feign.StoreLocationFeign;
import com.shj.mpms.feign.category.CategoryFeign;
import com.shj.oms.domain.OrderAttachs;
import com.shj.oms.domain.OrderDetail;
import com.shj.oms.domain.OrderInfo;
import com.shj.oms.domain.contants.MaterialType;
import com.shj.oms.domain.contants.OrderSourceType;
import com.shj.oms.domain.contants.OrderType;
import com.shj.oms.domain.request.OrderDetailRequest;
import com.shj.oms.domain.request.OrderRequest;
import com.shj.oms.domain.request.PmsCancelOrderRequest;
import com.shj.oms.domain.response.BatchOrRepairOrderResponse;
import com.shj.oms.domain.response.PmsCancelOrderResponse;
import com.shj.oms.domain.response.PmsOrderDetailResponse;
import com.shj.oms.domain.response.PmsOrderResponse;
import com.shj.oms.domain.response.PmsOrderStateDetailResponse;
import com.shj.oms.domain.response.PmsOrderStateResponse;
import com.shj.oms.feign.OrderDetailFeign;
import com.shj.oms.feign.OrderInfoFeign;
import com.shj.oms.feign.PmsOrderFeign;
import com.shj.pms.domain.ConstructionInfo;
import com.shj.pms.domain.MaterialCheckRecord;
import com.shj.pms.domain.ProjectExpensePayment;
import com.shj.pms.domain.ProjectExpensePaymentDetail;
import com.shj.pms.domain.ProjectExpensePaymentSource;
import com.shj.pms.domain.ProjectInfo;
import com.shj.pms.domain.ProjectMaterial;
import com.shj.pms.domain.ProjectMaterialTrace;
import com.shj.pms.domain.ProjectMaterialType;
import com.shj.pms.domain.ProjectOrder;
import com.shj.pms.domain.ProjectOrderDetail;
import com.shj.pms.domain.ProjectPicking;
import com.shj.pms.domain.ProjectPickingDetail;
import com.shj.pms.domain.ProjectPickingMaterialChain;
import com.shj.pms.domain.ProjectPlanTask;
import com.shj.pms.domain.ProjectSoftStartWork;
import com.shj.pms.domain.SaleCourseManage;
import com.shj.pms.domain.TaskMeterMaterialType;
import com.shj.pms.domain.contants.CabinetType;
import com.shj.pms.domain.contants.OrderResponseState;
import com.shj.pms.domain.contants.PmsRedisPrefix;
import com.shj.pms.domain.contants.ProjectCompletedGiftOrderMaterial;
import com.shj.pms.domain.contants.ProjectCustomerType;
import com.shj.pms.domain.contants.ProjectStatus;
import com.shj.pms.domain.contants.ProjectType;
import com.shj.pms.domain.contants.RuleCode;
import com.shj.pms.domain.contants.SaleCourseManageContants;
import com.shj.pms.domain.enums.BizType;
import com.shj.pms.domain.enums.PaymentSourceType;
import com.shj.pms.domain.request.*;
import com.shj.pms.domain.request.saleCourse.ProjectSaleCourseOrderRequest;
import com.shj.pms.domain.response.PaymentDto;
import com.shj.pms.domain.response.PmProjectOrderCheckExcessDto;
import com.shj.pms.domain.response.ProjectOrderDetailResponse;
import com.shj.pms.domain.response.ProjectOrderResponse;
import com.shj.pms.domain.vo.ProjectControlCostVo;
import com.shj.pms.domain.vo.ProjectExcessOrderRequestVo;
import com.shj.pms.domain.vo.ProjectExcessSaleCourseOrderRequestVo;
import com.shj.pms.domain.vo.ProjectOrderAndDetailVo;
import com.shj.pms.domain.vo.ProjectOrderRequestVo;
import com.shj.pms.domain.vo.ProjectPaymentVo;
import com.shj.pms.domain.vo.SaleCourseManageCreateOrderVo;
import com.shj.pms.domain.vo.SpecialAmountVo;
import com.shj.pms.domain.vo.thread.ThreadSkuDataInfoPrice;
import com.shj.pms.jms.produce.PmsOmsProduce;
import com.shj.pms.repository.ConstructionInfoRepository;
import com.shj.pms.repository.MaterialCheckRecordRepository;
import com.shj.pms.repository.ProjectExpensePaymentRepository;
import com.shj.pms.repository.ProjectExpensePaymentSourceRepository;
import com.shj.pms.repository.ProjectInfoRepository;
import com.shj.pms.repository.ProjectMaterialRepository;
import com.shj.pms.repository.ProjectMaterialTraceRepository;
import com.shj.pms.repository.ProjectMaterialTypeRepository;
import com.shj.pms.repository.ProjectOrderDetailRepository;
import com.shj.pms.repository.ProjectOrderRepository;
import com.shj.pms.repository.ProjectPickingDetailRepository;
import com.shj.pms.repository.ProjectPickingMaterialChainRepository;
import com.shj.pms.repository.ProjectPickingRepository;
import com.shj.pms.repository.ProjectPlanTaskRepository;
import com.shj.pms.repository.ProjectSoftStartWorkRepository;
import com.shj.pms.repository.SaleCourseManageRepository;
import com.shj.pms.repository.TaskMeterMaterialTypeRepository;
import com.shj.pms.service.ProjectExpensePaymentService;
import com.shj.pms.service.ProjectInfoService;
import com.shj.pms.service.ProjectInstallService;
import com.shj.pms.service.ProjectMaterialInfoSearchService;
import com.shj.pms.service.ProjectOrderService;
import com.shj.pms.service.ProjectPickingService;
import com.shj.pms.service.ProjectSettlementService;
import com.shj.pms.service.SaleCourseManageService;
import com.shj.tams.domain.CostData;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * <B>系统名称：PMS交付系统系统</B><BR>
 * <B>模块名称：PMS-SERVICE</B><BR>
 * <B>中文类名：订单申请表 业务实现类</B><BR>
 * <B>概要说明：订单申请表 业务实现类</B><BR>
 * <B>@version：v1.0</B><BR>
 * <B>版本		修改人		备注</B><BR>
 *
 * @author : zhangzhiwei
 * @date : 2018年07月16日
 */
@Service
public class ProjectOrderServiceImpl extends BaseEntityServiceImpl<ProjectOrder> implements ProjectOrderService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectOrderServiceImpl.class);
    private static final String BIZ_KEY = "PMS_ORDER";
    @Autowired
    private ProjectOrderRepository orderRepository;
    @Autowired
    private DataDictionaryServiceClient dataDictionaryServiceClient;
    @Autowired
    private ProjectMaterialRepository materialRepository;
    @Autowired
    private CodeRuleFeign codeRuleFeign;
    @Autowired
    private SkuFeign skuFeign;
    @Autowired
    private ProjectInfoRepository projectInfoRepository;
    @Autowired
    private ProjectOrderDetailRepository orderDetailRepository;
    @Autowired
    private ProjectMaterialTypeRepository materialTypeRepository;
    @Autowired
    private ProjectPlanTaskRepository planTaskRepository;
    @Autowired
    private TaskMeterMaterialTypeRepository taskMeterMaterialTypeRepository;
    @Autowired
    private ProjectMaterialTraceRepository traceRepository;
    @Autowired
    private ProjectPickingRepository pickingRepository;
    @Resource
    private ProjectPickingService pickingService;
    @Resource
    private ProjectInstallService installService;
    @Autowired
    private ProjectPickingDetailRepository pickingDetailRepository;
    @Resource
    private CostControlConfigFeign costControlConfigFeign;
    @Autowired
    private MaterialCheckRecordRepository recordRepository;
    @Autowired
    private ProjectPickingMaterialChainRepository chainRepository;
    @Autowired
    private ProjectExpensePaymentSourceRepository sourceRepository;
    @Resource
    private PmsOmsProduce produce;
    @Resource
    private OrderInfoFeign orderInfoFeign;
    @Resource
    private PmsOrderFeign pmsOrderFeign;
    @Resource
    private ProjectExpensePaymentRepository expensePaymentRepository;
    @Resource
    private BaseBizOpFeign bizOpFeign;
    @Lazy
    @Resource
    private BuildingInfoFeign buildingInfoFeign;
    @Resource
    private RegionFeign regionFeign;
    @Lazy
    @Resource
    private HouseInfoServiceClient houseInfoServiceClient;
    @Lazy
    @Resource
    private BaseBizOpFeign baseBizOpFeign;
    @Autowired
    private ProjectExpensePaymentRepository paymentRepository;
    @Autowired
    private ProjectExpensePaymentSourceRepository paymentSourceRepository;
    @Resource
    private ProjectExpensePaymentService paymentService;
    @Autowired
    private InventoryFeign inventoryFeign;
    @Resource
    private ElecContractAttachFeign elecContractAttachFeign;
    @Autowired
    private BizopContractFeign bizopContractFeign;
    @Autowired
    private PurchasePriceFeign purchasePriceFeign;
    @Resource
    private com.shj.tams.feign.CostDataFeign tamsCostFeign;
    @Autowired
    private StoreLocationFeign storeLocationFeign;
    @Resource
    private BidPmsFeign bidPmsFeign;
    @Resource
    private MaterialCheckRecordRepository checkRecordRepository;
    @Resource
    private ProjectSettlementService settlementService;
    @Autowired
    private SaleCourseManageRepository manageRepository;
    @Autowired
    private ProjectSoftStartWorkRepository softStartWorkRepository;
    @Autowired
    private CategoryFeign categoryFeign;
    @Autowired
    private SaleCourseManageService saleCourseManageService;
    @Autowired
    private OrderDetailFeign orderDetailFeign;
    @Autowired
    private BpmFeign bpmFeign;
    @Autowired
    private SmsClient smsClient;
    @Resource
    private ProjectMaterialInfoSearchService materialInfoSearchService;
    @Autowired
    private RedissonClient redisson;
    @Resource
    private ConstructionInfoRepository constructionInfoRepository;
    @Resource
    private ProjectInfoService infoService;
    @Resource
    private ReceiptApplyClient receiptApplyClient;



    @Value("${alipay.payment.url}")
    private String paymentUrl;

    @Value("${alipay.payment.notifyUrl}")
    private String paymentNotifyUrl;
    
    @Value("${vms.remoteFeeSkuCode}")
    private String remoteFeeSkuCode;    

    @Override
    public Page<ProjectOrderResponse> findProjectOrderList(
            @RequestBody Page<ProjectOrderResponse> page
            , @RequestParam("materialType") String materialType
            , @RequestParam("projectId") Long projectId
            , @RequestParam("status") String status) {
        List<ProjectOrderResponse> orderResponses = Lists.newArrayList();
        EntityWrapper<ProjectOrder> ew = new EntityWrapper<>();
        ProjectOrder entity = new ProjectOrder();
        if (StringUtils.isNotEmpty(status)) {
            entity.setStatus(status);
        }
        entity.setProjectId(projectId);
        entity.setMaterialTypeCode(materialType);
        ew.setEntity(entity);
        ew.orderBy("id",false);
        Page<ProjectOrder> orderPage = new Page<>();
        orderPage.setSize(page.getSize());
        orderPage.setCurrent(page.getCurrent());
        orderPage = orderRepository.selectPage(orderPage, ew);
        if (!ObjectUtils.isEmpty(orderPage.getRecords())) {
            for (ProjectOrder order : orderPage.getRecords()) {
                ProjectOrderResponse response = new ProjectOrderResponse();
                BeanUtils.copy(order, response);
                Map<String, String> taxpayerMap = dataDictionaryServiceClient.getDictListByKeys(new String[]{order.getStatus()});
                String statusName = taxpayerMap.get(order.getStatus());
                response.setStatusName(statusName);
                response.setExpectUseDate(DateUtil.date2Str(order.getExpectUseDate(), "yyyy-MM-dd"));
                response.setCreatedDate(DateUtil.date2Str(order
                        .getCreatedTime(), "yyyy-MM-dd"));
                response.setMaterialTypeName(getMaterialType(order.getId()));

                orderResponses.add(response);
            }
        }
        page.setTotal(orderResponses.size());
        page.setRecords(orderResponses);
        return page;
    }

    @Override
    public ProjectOrderResponse findOrderDetail(@PathVariable("orderId") Long orderId) {
        ProjectOrderResponse orderResponse = new ProjectOrderResponse();
        ProjectOrder order = orderRepository.selectById(orderId);
        BeanUtils.copy(order, orderResponse);
        orderResponse.setId(order.getId());
        Map<String, String> taxpayerMap = dataDictionaryServiceClient.getDictListByKeys(new String[]{order.getStatus()});
        String statusName = taxpayerMap.get(order.getStatus());
        orderResponse.setStatusName(statusName);
        orderResponse.setCreatedDate(DateUtil.date2Str(order
                .getCreatedTime(), "yyyy-MM-dd"));
        orderResponse.setExpectUseDate(DateUtil.date2Str(order.getExpectUseDate(),"yyyy-MM-dd"));
        orderResponse.setEmergencyFlag(order.getEmergencyFlag());
        orderResponse.setOrderAmount("0");
        orderResponse.setPayAmount("0");
        BigDecimal pickingAmount = BigDecimal.ZERO;

        List<ProjectOrderDetailResponse> orderResponses = Lists.newArrayList();
        orderResponse.setOrderDetailList(orderResponses);
        List<ProjectOrderDetail> orderDetails = orderRepository.selectOrderDetail(orderId);
        if (!ObjectUtils.isEmpty(orderDetails)) {
            for (ProjectOrderDetail orderDetail : orderDetails) {
                ProjectOrderDetailResponse response = new ProjectOrderDetailResponse();
                ProjectMaterial material = materialRepository.selectById(orderDetail.getProjectMaterialId());
                BeanUtils.copy(orderDetail, response);
                response.setOrderDetailId(orderDetail.getId());
                response.setSpecialCode(material.getSpecialCode());
                response.setMaterialCode(material.getMaterialCode());
                response.setMaterialName(material.getMaterialName());
                response.setDesigerRemark(orderDetail.getDesignerRemark());
                response.setRemark(orderDetail.getRemark());
                response.setSpace(orderDetail.getPosition());
                response.setConvertPlanNum(orderDetail.getOrderConvertNum().stripTrailingZeros().toPlainString());
                response.setPlanNum(orderDetail.getOrderNum().stripTrailingZeros().toPlainString());
                Map<String, String> detailStatusMap = dataDictionaryServiceClient.getDictListByKeys(new String[]{orderDetail.getStatus()});
                if(ObjectUtils.isEmpty(detailStatusMap)){
                    response.setStatusName("已取消");
                }else {
                    response.setStatusName(detailStatusMap.get(orderDetail.getStatus()));
                }
                response.setCancelReason(orderDetail.getCancelReason());
                pickingAmount = pickingAmount.add(orderDetail.getOrderNum().multiply(material.getProManagerPrice()));
                orderResponses.add(response);
            }
        }
        orderResponse.setOrderAmount(pickingAmount.stripTrailingZeros().toPlainString());

        //查询领料单关联的支付单
        EntityWrapper<ProjectExpensePaymentSource> ewPay = new EntityWrapper<>();
        ewPay.where("biz_id=" + orderId + " and biz_type='" + BizType.PROJECT_ORDER.getType() + "'");
        ProjectExpensePaymentSource paymentSource = paymentSourceRepository.selectOne(ewPay);
        if(!Objects.isNull(paymentSource)){
            ProjectExpensePayment payment = paymentRepository.selectById(paymentSource.getPaymentId());
            Objects.requireNonNull(payment,"支付订单有误!");
            orderResponse.setPayAmount(payment.getPaymentCost().stripTrailingZeros().toPlainString());
        }

        return orderResponse;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ProjectOrderResponse addProjectOrder(@RequestBody ProjectOrderRequestVo request) {
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单申请参数: [" + JSON.toJSONString(request) + "]######");

        final ProjectInfo info = projectInfoRepository.selectById(request.getProjectId());
        if(!ProjectStatus.pUnderConstruction.equals(info.getProjectStatus()) && !ProjectStatus.pReady.equals(info.getProjectStatus())){
            throw new BusinessException("项目必须是在建或者准备期才可以操作下单!");
        }

        if(!ProjectType.SOFT.equals(info.getType())){
            CostData costData = new CostData();
            costData.setIsDeleted(0);
            costData.setBizOpId(info.getTrackId());
            costData.setAccountType(DictionaryConstants.ACCOUNT_TYPE_SEND_PACK);
            costData.setState(DictionaryConstants.COST_COMPLETE);
            List<CostData> costDataList = tamsCostFeign.list(costData);

            if (ObjectUtils.isEmpty(costDataList)) {
                LOGGER.error("######[ProjectOrderServiceImpl.addProjectOrder]发包查询失败：项目经理发包表为空【" + info.getContractCode()+"]######");
                throw new BusinessException("项目经理未确认接包！商机ID[" + info.getTrackId() +"]");
            }
        }
        ProjectPlanTask projectPlanTask = null;
        //ProjectPlanTask projectPlanTask = planTaskRepository.selectOneByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());
        List<ProjectPlanTask> projectPlanTaskList = planTaskRepository.selectListByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());
        if(!ObjectUtils.isEmpty(projectPlanTaskList)){
            projectPlanTask = projectPlanTaskList.get(0);
        }
        List<Long> newMaterialList = Lists.newArrayList();
        ProjectOrderAndDetailVo orderAndDetailVo = new ProjectOrderAndDetailVo();
        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        //1.插入订单申请数据
        List<ProjectOrderDetail> orderDetailList = insertOrderAndDetails(request, newMaterialList, orderAndDetailVo,materialMap,projectPlanTask,info);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]保存订单明细，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        //2.更新计划任务状态
        //updateProjectPlanTask(request,projectPlanTask);
        updateProjectPlanTask(request,projectPlanTaskList);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单更新计划任务，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //3.更新项目主材表数据
        updateProjectMaterial(orderDetailList, request, newMaterialList,materialMap);
        LOGGER.info("######订单更新主材数据，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //4.更新物料类别状态为已下单
        updateMaterialType(request);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOSyncToPmsHardOrderEventrder]订单更新物料类别状态，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //5.构建oms订单所需参数对象
        OrderRequest orderRequest = buildOmsOrderReceiveVo(orderAndDetailVo,request);

        //6.同步订单到OMS
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统开始，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");
        produce.sendOrderToOms(orderRequest);
        //SyncToPmsHardOrderEvent event = new SyncToPmsHardOrderEvent(this,orderAndDetailVo.getOrder().getId(), request.getEmployeeCode(),request.getEmployeeName(),orderRequest);
        //applicationEventPublisher.publishEvent(event);
        //LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统结束，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统结束，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        String jsonStr = JSONObject.toJSONString(orderRequest);
        LOGGER.info("[ProjectOrderServiceImpl.addProjectOrder]报文测试：[" + jsonStr+"]");

        return null;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ProjectOrderResponse addExcessProjectOrder(@RequestBody ProjectExcessOrderRequestVo requestVo) {

        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]订单申请参数: [" + JSON.toJSONString(requestVo) + "]######");

        ProjectOrderRequestVo request = requestVo.getRequest();
        List<Long> newMaterialList = Lists.newArrayList();
        ProjectOrderAndDetailVo orderAndDetailVo = new ProjectOrderAndDetailVo();
        Map<Long, ProjectMaterial> materialMap = new HashMap<>();

        final ProjectInfo info = projectInfoRepository.selectById(request.getProjectId());
        if(!ProjectStatus.pUnderConstruction.equals(info.getProjectStatus()) && !ProjectStatus.pReady.equals(info.getProjectStatus())){
            throw new BusinessException("项目必须是在建或者准备期才可以操作下单!");
        }

        ProjectPlanTask projectPlanTask = null;
        //ProjectPlanTask projectPlanTask = planTaskRepository.selectOneByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());
        List<ProjectPlanTask> projectPlanTaskList = planTaskRepository.selectListByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());
        if(!ObjectUtils.isEmpty(projectPlanTaskList)){
            projectPlanTask = projectPlanTaskList.get(0);
        }

        //1.插入订单申请数据
        List<ProjectOrderDetail> orderDetailList = insertOrderAndDetails(request, newMaterialList, orderAndDetailVo,materialMap,projectPlanTask,info);
        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]保存订单明细，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        //2.更新计划任务状态
        updateProjectPlanTask(request,projectPlanTaskList);
        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]订单更新计划任务，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        //3.更新项目主材表数据
        updateProjectMaterial(orderDetailList, request, newMaterialList,materialMap);
        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]订单更新主材数据，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //4.更新物料类别状态为已下单
        updateMaterialType(request);
        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]订单更新物料类别状态，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        //5.更新超额支付关联关系
        updateOrderPaymentSource(requestVo, orderAndDetailVo);
        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]订单更新支付关联信息，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        //6.同步订单到OMS
        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]订单同步消息至OMS系统开始，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");
        produce.sendOrderToOms(buildOmsOrderReceiveVo(orderAndDetailVo,requestVo.getRequest()));
        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectOrder]订单同步消息至OMS系统结束，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        return null;
    }

    @Override
    @Transactional
    public void addExcessProjectSaleCourseOrder(@RequestBody ProjectExcessSaleCourseOrderRequestVo request) {

        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectSaleCourseOrder]售中补件订单申请参数: [" + JSON.toJSONString(request) + "]######");

        //获取售中补件业务ID
        Long bizId = request.getSource().getBizId();
        SaleCourseManageCreateOrderVo createOrderVo = new SaleCourseManageCreateOrderVo();
        createOrderVo.setId(bizId);
        createOrderVo.setUserCode(request.getEmployeeCode());
        createOrderVo.setUserName(request.getEmployeeName());

        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectSaleCourseOrder]售中补件订单申请 START]######");

        saleCourseManageService.createOrder(createOrderVo);

        LOGGER.info("######[ProjectOrderServiceImpl.addExcessProjectSaleCourseOrder]售中补件订单申请 END]######");

    }

    @Override
    @Transactional
    public void addProjectSaleCourseOrder(@RequestBody ProjectSaleCourseOrderRequest request) {

        LOGGER.info("######[ProjectOrderServiceImpl.addProjectSaleCourseOrder]售中补件订单申请参数: [" + JSON.toJSONString(request) + "]######");
        String orderId = DigestUtils.md5Hex(getContentBytes(JSON.toJSONString(request), "UTF-8"));
        String key = PmsRedisPrefix.ORDER_PREFIX + "_" + orderId + "_" + request.getSaleId() + "_" + request.getEmployeeCode();
        LOGGER.info("KEY:" + key);
        RLock lock = redisson.getLock(key);
        boolean res = false;

        try {
            // 尝试加锁，最多等待100秒，上锁以后10秒自动解锁
            res = lock.tryLock(0, 30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new BusinessException("系统异常，请联系系统管理员处理,售中业务ID:【" + request.getSaleId() +"】");
        }
        if (!res){
            throw new BusinessException("操作太频繁,请稍后再试!");
        }

        //获取售中补件业务ID
        Long bizId = request.getSaleId();
        SaleCourseManageCreateOrderVo createOrderVo = new SaleCourseManageCreateOrderVo();
        createOrderVo.setId(bizId);
        createOrderVo.setUserCode(request.getEmployeeCode());
        createOrderVo.setUserName(request.getEmployeeName());

        LOGGER.info("######[ProjectOrderServiceImpl.addProjectSaleCourseOrder]售中补件订单申请 START]######");

        saleCourseManageService.createOrder(createOrderVo);

        LOGGER.info("######[ProjectOrderServiceImpl.addProjectSaleCourseOrder]售中补件订单申请 END]######");

    }

    @Override
    public void rejectProjectSaleCourseOrder(@RequestBody ProjectSaleCourseOrderRequest request) {
        Long bizId = request.getSaleId();
        SaleCourseManageCreateOrderVo entity = new SaleCourseManageCreateOrderVo();
        entity.setId(bizId);
        entity.setUserCode(request.getEmployeeCode());
        entity.setUserName(request.getEmployeeName());

        Long id = entity.getId();
        if(id==null){
            throw new InvalidParameterException("主键id不能为空");
        }
        SaleCourseManage saleCourseManageDb = saleCourseManageService.selectById(id);
        if(saleCourseManageDb==null){
            throw new InvalidParameterException("根据主键id查询数据为空");
        }
        if(StringUtils.isBlank(entity.getUserCode())){
            throw new InvalidParameterException("创建人账号不能为空");
        }
        if(StringUtils.isBlank(entity.getUserName())){
            throw new InvalidParameterException("创建人名称不能为空");
        }
        if(!saleCourseManageDb.getSaleType().equals(SaleCourseManageContants.saleType.saleBefore.toString())){


            //查询当前节点
            ValidateFlowIsExistRequest request1=new ValidateFlowIsExistRequest();
            request1.setBussId(saleCourseManageDb.getId().toString());//entity.getCode()
            request1.setKeyField("id");
            request1.setFlowKeyList(Arrays.asList(DictionaryConstants.SALE_COURSE_PROCESS));
            List<CurrentNodeResponse> taskInfo = bpmFeign.getRunTaskInfo(request1);
            if(taskInfo==null||taskInfo.isEmpty()){
                throw new InvalidParameterException("查询无此流程节点");
            }else{
                //发起流程
                CurrentNodeResponse currentNodeResponse = taskInfo.get(0);
                List<String> auditorList = currentNodeResponse.getAuditorList();
                if(!auditorList.contains(saleCourseManageDb.getVendorCode())){
                    //throw new InvalidParameterException("节点审批人和查询出的审批人不一致");
                }

                FlowCheckRequest<Object> flowCheckRequest=new FlowCheckRequest<>();
                flowCheckRequest.setTaskId(currentNodeResponse.getTaskId());
                flowCheckRequest.setFlowKey(DictionaryConstants.SALE_COURSE_PROCESS);
                flowCheckRequest.setState("2");

                ProjectInfo info = projectInfoRepository.selectByContractCode(saleCourseManageDb.getProjectCode());
                UnifiedProfile unifiedProfile = new UnifiedProfile();
                unifiedProfile.setWorkNumber(WorkNumberUtil.getLaborWorkNumber(info.getProjectManagerCode(),info.getProjectManagerName()));
                flowCheckRequest.setAuditor(unifiedProfile);
                Map<String, Object> map = new HashMap<>();
                map.put("state", "2");
                flowCheckRequest.setVariableMap(map);
                bpmFeign.checkProcess(flowCheckRequest);
            }
        }
    }

    /**
     * 单独下单接口针对软装
     * @param id
     * @param auditor
     * @param auditorName
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void addSoftProjectOrder(@RequestParam("id") Long id,
                                    @RequestParam("auditor")String auditor,
                                    @RequestParam("auditorName")String auditorName){
        ProjectInfo projectInfo = projectInfoRepository.selectById(id);
        if(ObjectUtils.isEmpty(projectInfo)){
            throw new BusinessException("还未发起开工流程!");
        }
        EntityWrapper<ProjectSoftStartWork> softEntityWrapper = new EntityWrapper<>();
        softEntityWrapper.where("is_deleted = 0 and track_id = " + projectInfo.getTrackId());
        ProjectSoftStartWork softStartWork = softStartWorkRepository.selectOne(softEntityWrapper);
        if(UtilTool.isNull(softStartWork)){
            throw new BusinessException("还未发起软装开工流程!");
        }
        //过滤之前已经下过单--查询初始状态的物料
        EntityWrapper<ProjectMaterial> materialEntityWrapper = new EntityWrapper<>();
        ProjectMaterial param = new ProjectMaterial();
        param.setProjectId(projectInfo.getId());
        param.setIsDeleted(0);
        materialEntityWrapper.setEntity(param);
        param.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
        List<ProjectMaterial> materialList = materialRepository.selectList(materialEntityWrapper);
        if(ObjectUtils.isEmpty(materialList)){
            throw new BusinessException("不存在要下单的数据：[" + projectInfo.getContractCode()+"]");
        }
        Set<String> skuCodeSet = Sets.newHashSet();
        List<BidPmsBudgetDtlRequestParam> budgetDtlRequestParamList = Lists.newArrayList();
        for (ProjectMaterial item : materialList) {
            skuCodeSet.add(item.getMaterialCode());

            BidPmsBudgetDtlRequestParam budgetDtlRequestParam = new BidPmsBudgetDtlRequestParam();
            budgetDtlRequestParam.setBudgetDtlId(item.getComfirmDetailId());
            budgetDtlRequestParam.setBudgetPackDtlId(item.getPackageId());
            budgetDtlRequestParam.setId(item.getId());
            budgetDtlRequestParamList.add(budgetDtlRequestParam);
        }
        //Map<String, PurchasePrice> skuPriceMap = new HashMap<>();
        //List<String> skuCodes = new ArrayList<>(skuCodeSet);
        //StoreLocation location = findStoreLocation(projectInfo.getCompanyCode());
        //skuPriceMap = buildSkuPriceMap(skuCodes, location.getId());
        //checkNewSkuInventoryExisted(materialList, skuPriceMap, location);

        //查询所有的品类
        EntityWrapper<ProjectMaterialType> entityWrapper = new EntityWrapper<>();
        ProjectMaterialType materialTypeParam = new ProjectMaterialType();
        materialTypeParam.setProjectId(projectInfo.getId());
        materialTypeParam.setIsDeleted(0);
        entityWrapper.setEntity(materialTypeParam);
        List<ProjectMaterialType> materialTypeList = materialTypeRepository.selectList(entityWrapper);
        if(ObjectUtils.isEmpty(materialTypeList)){
            throw new BusinessException("不存在要下单的数据请核查：[" + projectInfo.getContractCode()+"]");
        }

        //多线程查询物料相关数据
        Map<Long,BidPmsBudgetDtlVo> dtlVoMap = bidPmsFeign.getBudgetDetails(budgetDtlRequestParamList);
        Map<String,SkuRelationDataVo> skuRelationDataVoMap = materialInfoSearchService.getSkuDataInfo(skuCodeSet,null);
        Map<String,ThreadSkuDataInfoPrice> skuDataInfoPriceMap = materialInfoSearchService.getSkuInventoryInfo(skuCodeSet,projectInfo.getCompanyCode());
        checkNewSkuInventoryExisted(materialList, skuDataInfoPriceMap);

        List<OrderRequest> orderRequestList = Lists.newArrayList();
        for(ProjectMaterialType materialType : materialTypeList){
            EntityWrapper<ProjectMaterial> projectMaterialEntityWrapper = new EntityWrapper<>();
            ProjectMaterial materialParam = new ProjectMaterial();
            materialParam.setMaterialMiddleType(materialType.getMaterialType());
            materialParam.setIsDeleted(0);
            materialParam.setProjectId(projectInfo.getId());
            materialParam.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
            projectMaterialEntityWrapper.setEntity(materialParam);
            List<ProjectMaterial> projectMaterialList = materialRepository.selectList(projectMaterialEntityWrapper);
            if(ObjectUtils.isEmpty(projectMaterialList)){
                continue;
            }
            List<ProjectOrderDetailResquest> orderDetailResquestList = Lists.newArrayList();
            for(ProjectMaterial mat : projectMaterialList){
                if(mat.getStatus().equals(PmsMaterialStatus.M_CANCELED.getCode())){
                    continue;
                }

                //PurchasePrice price = skuPriceMap.get(mat.getMaterialCode());
                // 设置明细是否入库属性
                //Inventory inventory = inventoryFeign.findInventory(mat.getMaterialCode(), price.getVendorCode(), location.getCode());
                ThreadSkuDataInfoPrice inventory = skuDataInfoPriceMap.get(mat.getMaterialCode());

                if(inventory.getPreStoreFlag()){
                    continue;
                }
                ProjectOrderDetailResquest orderDetail = new ProjectOrderDetailResquest();
                orderDetail.setMaterialCode(mat.getMaterialCode());
                orderDetail.setMaterialName(mat.getMaterialName());
//                BidPmsBudgetDtlRequestParam budgetDtlRequestParam = new BidPmsBudgetDtlRequestParam();
//                budgetDtlRequestParam.setBudgetDtlId(mat.getComfirmDetailId());
//                budgetDtlRequestParam.setBudgetPackDtlId(mat.getPackageId());
//                BidPmsBudgetDtlVo dtlVo =bidPmsFeign.getBudgetDetail(budgetDtlRequestParam);
                BidPmsBudgetDtlVo dtlVo = dtlVoMap.get(mat.getId());

                BigDecimal planNum = BigDecimal.ZERO;
                if(ObjectUtils.isEmpty(dtlVo)){
                    continue;
                }else {

                    //项目经理价
                    orderDetail.setProManagerPrice(dtlVo.getPmPrice().stripTrailingZeros().toPlainString());
                    //空间
                    orderDetail.setSpace(dtlVo.getSpaceName());
                    orderDetail.setDesigerRemark("");
                    if (!org.springframework.util.StringUtils.isEmpty(dtlVo.getDynaOptions())) {
                        StringBuffer dyAtt = new StringBuffer("");
                        JSONArray jsonArray = JSON.parseArray(dtlVo.getDynaOptions());
                        buildDtlDyAtt(dyAtt, jsonArray);
                        orderDetail.setDesigerRemark("主数据备注：" + UtilTool.convertNullToString(dtlVo.getMainRemark())
                                + ",打包数据备注：" + UtilTool.convertNullToString(dtlVo.getPackRemark()) + ",动态属性：" + dyAtt.toString()
                        );
                    } else {
                        orderDetail.setDesigerRemark("主数据备注：" + UtilTool.convertNullToString(dtlVo.getMainRemark())
                                + ",打包数据备注：" + UtilTool.convertNullToString(dtlVo.getPackRemark()));
                    }
                    //计划量
                    planNum = dtlVo.getPlanNumber() == null ? BigDecimal.ZERO : dtlVo.getPlanNumber();
                }
                if(planNum.compareTo(BigDecimal.ZERO) == 0){
                    continue;
                }
                //采购单位数量
                orderDetail.setOrderNum(planNum.stripTrailingZeros().toEngineeringString());
                //销售单位数量
                //SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(mat.getMaterialCode());
                SkuRelationDataVo skuRelationDataVo = skuRelationDataVoMap.get(mat.getMaterialCode());

                buildOrderDetailUnitInfo(orderDetail, planNum, skuRelationDataVo);
                orderDetail.setProManagerPrice(mat.getProManagerPrice().stripTrailingZeros().toEngineeringString());
                orderDetail.setProjectMaterialId(mat.getId());
                orderDetail.setProjectId(mat.getProjectId());
                orderDetail.setRemark("");
                orderDetailResquestList.add(orderDetail);
            }
            if(ObjectUtils.isEmpty(orderDetailResquestList)){
                throw new BusinessException("待订单明细为空，合同号：[" + "" + "]");
            }
            //包装下单明细
            ProjectOrderRequestVo request = new ProjectOrderRequestVo();
            request.setEmployeeCode(auditor);
            request.setProjectId(projectInfo.getId());
            request.setEmployeeName(auditorName);
            request.setEmployeeCode(auditor);
            request.setEmergencyFlag(0);
            request.setMaterialTypeCode(materialType.getMaterialType());
            request.setMaterialTypeName(materialType.getMaterialTypeName());
            request.setOrderDetailList(orderDetailResquestList);
            EntityWrapper<ProjectSoftStartWork> sofEW = new EntityWrapper<>();
            sofEW.where("is_deleted=0 and track_id = " + projectInfo.getTrackId());
            ProjectSoftStartWork projectSoftStartWork = softStartWorkRepository.selectOne(sofEW);
            request.setExpectUseDate(DateUtil.date2Str(projectSoftStartWork.getPreInvenrotyCompleteDate()));
            OrderRequest orderRequest = buildOmsOrderRequestForSoftOrder(request);
            orderRequestList.add(orderRequest);
        }

        if(orderRequestList.size()>0){
            for(OrderRequest orderRequest : orderRequestList){
                //6.同步订单到OMS
                LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统开始，单号[" + orderRequest.getSourceCode() + "]######");
                produce.sendOrderToOms(orderRequest);
                LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统结束，单号[" + orderRequest.getSourceCode() + "]######");


                String jsonStr = JSONObject.toJSONString(orderRequest);
                LOGGER.info("[ProjectOrderServiceImpl.addProjectOrder]报文测试：[" + jsonStr+"]");
            }
        }
    }

    @Override
    public void reAddSoftProjectOrder(@RequestParam("id") Long id,
                                      @RequestParam("date") String date,
                                      @RequestParam("auditor")String auditor,
                                      @RequestParam("auditorName")String auditorName) {

        ProjectInfo projectInfo = projectInfoRepository.selectById(id);
        if(ObjectUtils.isEmpty(projectInfo)){
            throw new BusinessException("还未发起开工流程!");
        }
        //过滤之前已经下过单--查询初始状态的物料
        EntityWrapper<ProjectMaterial> materialEntityWrapper = new EntityWrapper<>();
        ProjectMaterial param = new ProjectMaterial();
        param.setProjectId(projectInfo.getId());
        param.setIsDeleted(0);
        materialEntityWrapper.setEntity(param);
        materialEntityWrapper.where("status in('mOriginalStatus','mMeasured')");

        //param.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
        List<ProjectMaterial> materialList = materialRepository.selectList(materialEntityWrapper);
        if(ObjectUtils.isEmpty(materialList)){
            throw new BusinessException("不存在要下单的数据：[" + projectInfo.getContractCode()+"]");
        }
        Set<String> skuCodeSet = Sets.newHashSet();
        List<BidPmsBudgetDtlRequestParam> budgetDtlRequestParamList = Lists.newArrayList();
        for (ProjectMaterial item : materialList) {
            skuCodeSet.add(item.getMaterialCode());

            BidPmsBudgetDtlRequestParam budgetDtlRequestParam = new BidPmsBudgetDtlRequestParam();
            budgetDtlRequestParam.setBudgetDtlId(item.getComfirmDetailId());
            budgetDtlRequestParam.setBudgetPackDtlId(item.getPackageId());
            budgetDtlRequestParam.setId(item.getId());
            budgetDtlRequestParamList.add(budgetDtlRequestParam);
        }

        //查询所有的品类
        EntityWrapper<ProjectMaterialType> entityWrapper = new EntityWrapper<>();
        ProjectMaterialType materialTypeParam = new ProjectMaterialType();
        materialTypeParam.setProjectId(projectInfo.getId());
        materialTypeParam.setIsDeleted(0);
        entityWrapper.setEntity(materialTypeParam);
        List<ProjectMaterialType> materialTypeList = materialTypeRepository.selectList(entityWrapper);
        if(ObjectUtils.isEmpty(materialTypeList)){
            throw new BusinessException("不存在要下单的数据请核查：[" + projectInfo.getContractCode()+"]");
        }

        //多线程查询物料相关数据
        Map<Long,BidPmsBudgetDtlVo> dtlVoMap = bidPmsFeign.getBudgetDetails(budgetDtlRequestParamList);
        Map<String,SkuRelationDataVo> skuRelationDataVoMap = materialInfoSearchService.getSkuDataInfo(skuCodeSet,null);
        Map<String,ThreadSkuDataInfoPrice> skuDataInfoPriceMap = materialInfoSearchService.getSkuInventoryInfo(skuCodeSet,projectInfo.getCompanyCode());
        checkNewSkuInventoryExisted(materialList, skuDataInfoPriceMap);

        List<OrderRequest> orderRequestList = Lists.newArrayList();
        for(ProjectMaterialType materialType : materialTypeList){
            EntityWrapper<ProjectMaterial> projectMaterialEntityWrapper = new EntityWrapper<>();
            ProjectMaterial materialParam = new ProjectMaterial();
            materialParam.setMaterialMiddleType(materialType.getMaterialType());
            materialParam.setIsDeleted(0);
            materialParam.setProjectId(projectInfo.getId());
            //materialParam.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
            projectMaterialEntityWrapper.setEntity(materialParam);
            projectMaterialEntityWrapper.where("status in('mOriginalStatus','mMeasured')");

            List<ProjectMaterial> projectMaterialList = materialRepository.selectList(projectMaterialEntityWrapper);
            if(ObjectUtils.isEmpty(projectMaterialList)){
                continue;
            }
            List<ProjectOrderDetailResquest> orderDetailResquestList = Lists.newArrayList();
            for(ProjectMaterial mat : projectMaterialList){
                if(mat.getStatus().equals(PmsMaterialStatus.M_CANCELED.getCode())){
                    continue;
                }

                ThreadSkuDataInfoPrice inventory = skuDataInfoPriceMap.get(mat.getMaterialCode());

                if(inventory.getPreStoreFlag()){
                    continue;
                }
                ProjectOrderDetailResquest orderDetail = new ProjectOrderDetailResquest();
                orderDetail.setMaterialCode(mat.getMaterialCode());
                orderDetail.setMaterialName(mat.getMaterialName());
                BidPmsBudgetDtlVo dtlVo = dtlVoMap.get(mat.getId());

                BigDecimal planNum = BigDecimal.ZERO;
                if(ObjectUtils.isEmpty(dtlVo)){
                    continue;
                }else {
                    //项目经理价
                    orderDetail.setProManagerPrice(dtlVo.getPmPrice().stripTrailingZeros().toPlainString());
                    //空间
                    orderDetail.setSpace(dtlVo.getSpaceName());
                    orderDetail.setDesigerRemark("");
                    if (!org.springframework.util.StringUtils.isEmpty(dtlVo.getDynaOptions())) {
                        StringBuffer dyAtt = new StringBuffer("");
                        JSONArray jsonArray = JSON.parseArray(dtlVo.getDynaOptions());
                        buildDtlDyAtt(dyAtt, jsonArray);
                        orderDetail.setDesigerRemark("主数据备注：" + UtilTool.convertNullToString(dtlVo.getMainRemark())
                                + ",打包数据备注：" + UtilTool.convertNullToString(dtlVo.getPackRemark()) + ",动态属性：" + dyAtt.toString()
                        );
                    } else {
                        orderDetail.setDesigerRemark("主数据备注：" + UtilTool.convertNullToString(dtlVo.getMainRemark())
                                + ",打包数据备注：" + UtilTool.convertNullToString(dtlVo.getPackRemark()));
                    }
                    //计划量
                    planNum = dtlVo.getPlanNumber() == null ? BigDecimal.ZERO : dtlVo.getPlanNumber();
                }

                if(planNum.compareTo(BigDecimal.ZERO) == 0){
                    continue;
                }
                orderDetail.setOrderNum(planNum.stripTrailingZeros().toEngineeringString());

                SkuRelationDataVo skuRelationDataVo = skuRelationDataVoMap.get(mat.getMaterialCode());

                buildOrderDetailUnitInfo(orderDetail, planNum, skuRelationDataVo);
                orderDetail.setProManagerPrice(mat.getProManagerPrice().stripTrailingZeros().toEngineeringString());
                orderDetail.setProjectMaterialId(mat.getId());
                orderDetail.setProjectId(mat.getProjectId());
                orderDetail.setRemark("");
                orderDetailResquestList.add(orderDetail);
            }
            if(ObjectUtils.isEmpty(orderDetailResquestList)){
                throw new BusinessException("待订单明细为空，合同号：[" + "" + "]");
            }
            //包装下单明细
            ProjectOrderRequestVo request = new ProjectOrderRequestVo();
            request.setEmployeeCode(auditor);
            request.setProjectId(projectInfo.getId());
            request.setEmployeeName(auditor);
            request.setEmergencyFlag(0);
            request.setMaterialTypeCode(materialType.getMaterialType());
            request.setMaterialTypeName(materialType.getMaterialTypeName());
            request.setOrderDetailList(orderDetailResquestList);
            request.setExpectUseDate(date);
            //this.addProjectOrder(request);
            OrderRequest orderRequest = buildOmsOrderRequestForSoftOrder(request);
            orderRequestList.add(orderRequest);
        }

        if(orderRequestList.size()>0){
            for(OrderRequest orderRequest : orderRequestList){
                //6.同步订单到OMS
                LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统开始，单号[" + orderRequest.getSourceCode() + "]######");
                produce.sendOrderToOms(orderRequest);
                LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统结束，单号[" + orderRequest.getSourceCode() + "]######");


                String jsonStr = JSONObject.toJSONString(orderRequest);
                LOGGER.info("[ProjectOrderServiceImpl.addProjectOrder]报文测试：[" + jsonStr+"]");
            }
        }

    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void cancelSoftOrder(@RequestBody ProjectSoftOrderCancelRequest cancelRequest) {
        if(ObjectUtils.isEmpty(cancelRequest.getOrderCode())){
            throw new InvalidParameterException("订单单号不能为空!");
        }
        List<ProjectOrderDetail> omsOrderDetailList = orderDetailRepository.selectOrderDetailsByOmsOrderCode(cancelRequest.getOrderCode());
        if(ObjectUtils.isEmpty(omsOrderDetailList)){
            throw new BusinessException("未查到对应下单明细数据：【" + cancelRequest.getOrderCode() + "】");
        }
        ProjectOrder order = orderRepository.selectById(omsOrderDetailList.get(0).getOrderId());
        if (order.getStatus().equals(PmsOrderStatus.PO_APPLYING.getCode())) {
            throw new BusinessException("订单正在处理中，请稍后操作!");
        }
        if (order.getStatus().equals(PmsOrderStatus.PO_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }

        PmsCancelOrderRequest request = new PmsCancelOrderRequest();
        request.setCause(cancelRequest.getCancelReason());
        request.setOperator(cancelRequest.getEmployeeCode());
        request.setOperatorName(cancelRequest.getEmployeeName());
        request.setOrderCode(cancelRequest.getOrderCode());

        List<PmsCancelOrderResponse> cancelOrderResponseList = pmsOrderFeign.pmsCancelOrder(request);

        LOGGER.info("######[ProjectOrderServiceImpl.cancelPmsOrder.cancelOrderResponseList():[" + JSON.toJSONString(cancelOrderResponseList) + "]  ######");

        if(ObjectUtils.isEmpty(cancelOrderResponseList)){
            throw new BusinessException("订单取消失败!");
        }
        final List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());
        List<String> itemIds = cancelOrderResponseList.stream().filter(item->item.getCanceled()==true).map(PmsCancelOrderResponse::getSourceLineNo).collect(Collectors.toList());
        if(ObjectUtils.isEmpty(itemIds)){
            throw new BusinessException("商家已经接单无法取消!");
        }
        EntityWrapper<ProjectOrderDetail> ew = new EntityWrapper<>();
        ew.in("item_id", itemIds);
        final List<ProjectOrderDetail> cancelDetailList = orderDetailRepository.selectList(ew);


        if (orderDetailList.size() == omsOrderDetailList.size()) {
            cancelPmsOrderDataAll(order,cancelRequest);
        } else {
            //部分取消订单
            cancelPmsOrderDataAllPartial(cancelOrderResponseList,cancelDetailList,cancelRequest);
        }


    }

    @Override
    @Transactional
    public void addRemoteProjectOrder(@RequestParam("id") Long id,
                                      @RequestParam("auditor")String auditor,
                                      @RequestParam("auditorName")String auditorName,
                                      @RequestParam("remoteFee")String remoteFee,
                                      @RequestParam("vendorCode")String vendorCode) {
        ProjectMaterial material = materialRepository.selectById(id);
        if(UtilTool.isNull(material)){
            throw new BusinessException("远程费物料不存在，请联系系统管理员");
        }

        SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(material.getMaterialCode());
        List<ProjectOrderDetailResquest> orderDetailResquestList = Lists.newArrayList();
        ProjectOrderDetailResquest orderDetail = new ProjectOrderDetailResquest();
        orderDetail.setMaterialCode(material.getMaterialCode());
        orderDetail.setMaterialName(material.getMaterialName());
        //项目经理价
        orderDetail.setProManagerPrice(material.getProManagerPrice().stripTrailingZeros().toPlainString());

        //计划量
        //采购单位数量
        orderDetail.setOrderNum(remoteFee);

        if (skuRelationDataVo != null) {
            //采购单位 //销售单位
            List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
            //获取采购单位和销售单位根据类型
            if (!ObjectUtils.isEmpty(spuUnitList)) {
                for (SpuUnit unit : spuUnitList) {
                    if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                        BigDecimal convertNum = new BigDecimal(remoteFee).multiply(unit.getConvertRate()).setScale(unit.getDecimalLength(),BigDecimal.ROUND_HALF_UP);
                        orderDetail.setOrderConvertNum(convertNum.stripTrailingZeros().toPlainString());
                    }
                    if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                        orderDetail.setConvertUnit(unit.getUnitName());
                    }
                }
            }
        }
        orderDetail.setProManagerPrice(material.getProManagerPrice().stripTrailingZeros().toEngineeringString());
        orderDetail.setProjectMaterialId(material.getId());
        orderDetail.setProjectId(material.getProjectId());
        orderDetail.setRemark("供应商远程费");
        orderDetail.setVendorCode(vendorCode);
        orderDetailResquestList.add(orderDetail);

        Category category = categoryFeign.getById(skuRelationDataVo.getSpu().getCategoryId());
        Category middleCategory = categoryFeign.getById(category.getParentId());
        insertProjectOrder(orderDetailResquestList,auditor,auditorName,material.getProjectId(),middleCategory.getCode(),middleCategory.getName());

    }

    @Override
    public void sendOrderMsgToOms(@RequestBody OrderRequest orderRequest) {
        produce.sendOrderToOms(orderRequest);
    }

    @Override
    public void sendOrderListMsgToOms(@RequestBody List<Long> ids) {
        for(Long id : ids){
            ProjectOrder order = orderRepository.selectById(id);
            List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());
            ProjectInfo projectInfo = projectInfoRepository.selectById(order.getProjectId());

            OrderRequest omsOrderReceiveVo = new OrderRequest();
            omsOrderReceiveVo.setSourceType(OrderSourceType.APP);
            omsOrderReceiveVo.setProjectCode(projectInfo.getContractCode());
            omsOrderReceiveVo.setCompanyCode(projectInfo.getCompanyCode());

            List<OrderAttachs> attachs = Lists.newArrayList();
            orderDetailList.forEach(item->{
                if(attachs.size() == 0){
                    OrderAttachs att = new OrderAttachs();
                    att.setAttachUrl(item.getAttachmentUrl());
                    attachs.add(att);
                }

            });
            omsOrderReceiveVo.setAttachs(attachs);
            omsOrderReceiveVo.setCreator(order.getCreatedBy());
            omsOrderReceiveVo.setIsEmergency(order.getEmergencyFlag());
            omsOrderReceiveVo.setPalcePerson(projectInfo.getProjectManagerCode());
            omsOrderReceiveVo.setCreator(projectInfo.getProjectManagerCode());
            omsOrderReceiveVo.setProjectManager(projectInfo.getProjectManagerCode());
            omsOrderReceiveVo.setCustomer(projectInfo.getCustomerName());
            omsOrderReceiveVo.setPmTelephone(projectInfo.getProjectManagerPhone());
            omsOrderReceiveVo.setProjectManager(projectInfo.getProjectManagerName());
            if(projectInfo.getType().equals(ProjectType.SOFT)){
                BaseBizOpDetailResponseVoForPm responseVo= bizOpFeign.findDetailInfoByIdForPm(projectInfo.getTrackId(),true);
                omsOrderReceiveVo.setPalcePerson(responseVo.getDesignerDisplay());
                omsOrderReceiveVo.setLinkman(responseVo.getDesignerDisplay());
                omsOrderReceiveVo.setLinkTelephone(responseVo.getDesignerPhone());
                omsOrderReceiveVo.setCreator(responseVo.getDesigner());
            }else{
                omsOrderReceiveVo.setPalcePerson(projectInfo.getProjectManagerName());
                omsOrderReceiveVo.setLinkman(projectInfo.getProjectManagerName());
                omsOrderReceiveVo.setLinkTelephone(projectInfo.getProjectManagerPhone());
            }

            omsOrderReceiveVo.setAddress(projectInfo.getProjectName());
            omsOrderReceiveVo.setRemark(order.getRemark());
            omsOrderReceiveVo.setType(OrderType.PURCHASE);
            omsOrderReceiveVo.setRequiredArriveDate(new Date());
            omsOrderReceiveVo.setSourceCode(order.getOrderCode());
            buildOrderAddressInfo(projectInfo, omsOrderReceiveVo);
            List<OrderDetailRequest> detailVos = Lists.newArrayList();
            buildOrderDetailRequest(orderDetailList, detailVos,projectInfo);
            omsOrderReceiveVo.setVendorCode(orderDetailList.get(0).getVendorCode());
            omsOrderReceiveVo.setDetails(detailVos);

            this.sendOrderMsgToOms(omsOrderReceiveVo);
        }
    }

    @Override
    @Transactional
    public void addGiftMaterialOrder(@RequestParam("id") Long id,
                                     @RequestParam("date") String date,
                                     @RequestParam("auditor")String auditor,
                                     @RequestParam("auditorName")String auditorName) {
        try {
            ProjectInfo projectInfo = projectInfoRepository.selectById(id);
            if(ObjectUtils.isEmpty(projectInfo)){
                throw new BusinessException("还未发起开工流程!");
            }
            EntityWrapper<ProjectMaterial> entityWrapper = new EntityWrapper<>();
            entityWrapper.where("project_id = " + id + " and is_deleted = 0 and material_code in('" + ProjectCompletedGiftOrderMaterial.completion_package_one + "','" + ProjectCompletedGiftOrderMaterial.completion_package_two + "'");
            List<ProjectMaterial> materialList = materialRepository.selectList(entityWrapper);
            if(UtilTool.isNull(materialList)){
                return;
            }
            Set<String> skuCodeSet = Sets.newHashSet();
            List<BidPmsBudgetDtlRequestParam> budgetDtlRequestParamList = Lists.newArrayList();
            for (ProjectMaterial item : materialList) {
                skuCodeSet.add(item.getMaterialCode());

                BidPmsBudgetDtlRequestParam budgetDtlRequestParam = new BidPmsBudgetDtlRequestParam();
                budgetDtlRequestParam.setBudgetDtlId(item.getComfirmDetailId());
                budgetDtlRequestParam.setBudgetPackDtlId(item.getPackageId());
                budgetDtlRequestParam.setId(item.getId());
                budgetDtlRequestParamList.add(budgetDtlRequestParam);
            }
            //多线程查询物料相关数据
            Map<Long,BidPmsBudgetDtlVo> dtlVoMap = bidPmsFeign.getBudgetDetails(budgetDtlRequestParamList);
            Map<String,SkuRelationDataVo> skuRelationDataVoMap = materialInfoSearchService.getSkuDataInfo(skuCodeSet,null);
            Map<String,ThreadSkuDataInfoPrice> skuDataInfoPriceMap = materialInfoSearchService.getSkuInventoryInfo(skuCodeSet,projectInfo.getCompanyCode());
            checkNewSkuInventoryExisted(materialList, skuDataInfoPriceMap);

            List<OrderRequest> orderRequestList = Lists.newArrayList();
            List<ProjectOrderDetailResquest> orderDetailResquestList = Lists.newArrayList();
            List<ProjectPickingDetailAddParam> pickingDetailResquestList = Lists.newArrayList();
            for(ProjectMaterial material : materialList){
                if(material.getStatus().equals(PmsMaterialStatus.M_CANCELED.getCode())){
                    continue;
                }

                ThreadSkuDataInfoPrice inventory = skuDataInfoPriceMap.get(material.getMaterialCode());
                if(inventory.getPreStoreFlag()){
                    ProjectPickingDetailAddParam orderDetail = new ProjectPickingDetailAddParam();
                    orderDetail.setMaterialCode(material.getMaterialCode());
                    orderDetail.setMaterialName(material.getMaterialName());
                    BidPmsBudgetDtlRequestParam budgetDtlRequestParam = new BidPmsBudgetDtlRequestParam();
                    budgetDtlRequestParam.setBudgetDtlId(material.getComfirmDetailId());
                    budgetDtlRequestParam.setBudgetPackDtlId(material.getPackageId());
                    BidPmsBudgetDtlVo dtlVo = bidPmsFeign.getBudgetDetail(budgetDtlRequestParam);
                    BigDecimal planNum = BigDecimal.ZERO;
                    if(ObjectUtils.isEmpty(dtlVo)){
                        continue;
                    }else {
                        //项目经理价
                        orderDetail.setProManagerPrice(dtlVo.getPmPrice().stripTrailingZeros().toPlainString());
                        //计划量
                        planNum = dtlVo.getPlanNumber() == null ? BigDecimal.ZERO : dtlVo.getPlanNumber();
                    }
                    orderDetail.setInStoreFlag(inventory.getInStoreFlag()==true?1:0);
                    orderDetail.setPreStockFlag(inventory.getPreStoreFlag()==true?1:0);

                    List<ProjectOrderDetail> orderDetailList = orderRepository.selectListByProjectMaterialId(material.getId());
                    if(!ObjectUtils.isEmpty(orderDetailList)){
                        //如果已经下单的则默认是下单量，不允许修改
                        ProjectOrderDetail projectOrderDetail = orderDetailList.get(0);
                        if(!ObjectUtils.isEmpty(projectOrderDetail.getInStore()) && projectOrderDetail.getInStore()){
                            continue;
                        }
                        if (material.getOrderNum() != null && material.getOrderNum().compareTo(BigDecimal.ZERO) > 0) {
                            orderDetail.setPickingNum(material.getOrderNum().stripTrailingZeros().toPlainString());
                            orderDetail.setPickingConvertNum(material.getOrderConvertNum().stripTrailingZeros().toPlainString());
                            orderDetail.setInStoreFlag(1);
                            orderDetail.setPreStockFlag(0);
                        }
                    }else {
                        //采购单位数量
                        orderDetail.setPickingNum(planNum.stripTrailingZeros().toEngineeringString());
                        //销售单位数量
                        SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(material.getMaterialCode());
                        if (skuRelationDataVo != null) {
                            //采购单位 //销售单位
                            List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                            Integer convertDecimalLenth = 0;
                            BigDecimal convertRate = BigDecimal.ONE;
                            //获取采购单位和销售单位根据类型
                            if (!ObjectUtils.isEmpty(spuUnitList)) {
                                for (SpuUnit unit : spuUnitList) {
                                    if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                                        convertRate = unit.getConvertRate();
                                    }
                                    if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                                        convertDecimalLenth = unit.getDecimalLength();
                                    }
                                }
                            }
                            orderDetail.setPickingConvertNum((planNum.multiply(convertRate).setScale(convertDecimalLenth,BigDecimal.ROUND_HALF_UP)).stripTrailingZeros().toPlainString());
                        }
                    }
                    orderDetail.setProManagerPrice(material.getProManagerPrice().stripTrailingZeros().toEngineeringString());
                    orderDetail.setProjectMaterialId(material.getId());
                    orderDetail.setRemark("");
                    pickingDetailResquestList.add(orderDetail);
                }
                ProjectOrderDetailResquest orderDetail = new ProjectOrderDetailResquest();
                orderDetail.setMaterialCode(material.getMaterialCode());
                orderDetail.setMaterialName(material.getMaterialName());
                BidPmsBudgetDtlVo dtlVo = dtlVoMap.get(material.getId());

                BigDecimal planNum = BigDecimal.ZERO;
                if(ObjectUtils.isEmpty(dtlVo)){
                    continue;
                }else {
                    //项目经理价
                    orderDetail.setProManagerPrice(dtlVo.getPmPrice().stripTrailingZeros().toPlainString());
                    //空间
                    orderDetail.setSpace(dtlVo.getSpaceName());
                    orderDetail.setDesigerRemark("");
                    if (!org.springframework.util.StringUtils.isEmpty(dtlVo.getDynaOptions())) {
                        StringBuffer dyAtt = new StringBuffer("");
                        JSONArray jsonArray = JSON.parseArray(dtlVo.getDynaOptions());
                        buildDtlDyAtt(dyAtt, jsonArray);
                        orderDetail.setDesigerRemark("主数据备注：" + UtilTool.convertNullToString(dtlVo.getMainRemark())
                                + ",打包数据备注：" + UtilTool.convertNullToString(dtlVo.getPackRemark()) + ",动态属性：" + dyAtt.toString()
                        );
                    } else {
                        orderDetail.setDesigerRemark("主数据备注：" + UtilTool.convertNullToString(dtlVo.getMainRemark())
                                + ",打包数据备注：" + UtilTool.convertNullToString(dtlVo.getPackRemark()));
                    }
                    //计划量
                    planNum = dtlVo.getPlanNumber() == null ? BigDecimal.ZERO : dtlVo.getPlanNumber();
                }
                //采购单位数量
                orderDetail.setOrderNum(planNum.stripTrailingZeros().toEngineeringString());
                //销售单位数量
                SkuRelationDataVo skuRelationDataVo = skuRelationDataVoMap.get(material.getMaterialCode());

                buildOrderDetailUnitInfo(orderDetail, planNum, skuRelationDataVo);
                orderDetail.setProManagerPrice(material.getProManagerPrice().stripTrailingZeros().toEngineeringString());
                orderDetail.setProjectMaterialId(material.getId());
                orderDetail.setProjectId(material.getProjectId());
                orderDetail.setRemark("");
                orderDetailResquestList.add(orderDetail);
            }
            if(UtilTool.isNotNull(orderDetailResquestList)){
                //包装下单明细
                ProjectOrderRequestVo request = new ProjectOrderRequestVo();
                request.setEmployeeCode(auditor);
                request.setProjectId(projectInfo.getId());
                request.setEmployeeName(auditorName);
                request.setEmployeeCode(auditor);
                request.setEmergencyFlag(0);
                request.setMaterialTypeCode("11011");
                request.setMaterialTypeName("电器");
                request.setOrderDetailList(orderDetailResquestList);
                request.setExpectUseDate(DateUtil.date2Str(new Date(),"yyyy-MM-dd"));
                OrderRequest orderRequest = buildOmsOrderRequestForGfitOrder(request);
                orderRequestList.add(orderRequest);
            }

            if(orderRequestList.size()>0){
                for(OrderRequest orderRequest : orderRequestList){
                    //6.同步订单到OMS
                    LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统开始，单号[" + orderRequest.getSourceCode() + "]######");
                    produce.sendOrderToOms(orderRequest);
                    LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统结束，单号[" + orderRequest.getSourceCode() + "]######");


                    String jsonStr = JSONObject.toJSONString(orderRequest);
                    LOGGER.info("[ProjectOrderServiceImpl.addProjectOrder]报文测试：[" + jsonStr+"]");
                }
            }


            if(UtilTool.isNotNull(pickingDetailResquestList)){
                //包装领料明细
                ProjectPickingAddParam request = new ProjectPickingAddParam();
                request.setEmployeeCode(auditor);
                request.setProjectId(projectInfo.getId());
                request.setEmployeeName(auditorName);
                request.setExpectArrivalDate(DateUtil.date2Str(new Date(),"yyyy-MM-dd"));
                request.setDetails(pickingDetailResquestList);
                //校验库存
                //validateStock(request);
                pickingService.addProjectPicking(request);

            }

        }catch (Exception e){
            LOGGER.info("赠品订单下单失败："+e.getMessage());
        }

    }

    /**
     * 项目详情——订单列表
     * @param currentPage
     * @param pageSize
     * @return
     */
    @Override
    public Page<ProjectOrderResponse> getProjectOrderList(@RequestParam("projectId") Long projectId, @RequestParam("currentPage") int currentPage, @RequestParam("pageSize") int pageSize) {
        Page<ProjectOrderResponse> page = new Page(currentPage, pageSize);
        List<ProjectOrderResponse> list = new ArrayList<>();
        //查询订单
        Page<ProjectOrder> resultPage=new Page<>(currentPage,pageSize);
        List<ProjectOrder> result = orderRepository.getProjectOrderList(resultPage, projectId);
        resultPage.setRecords(result);
        List<ProjectOrder> records = resultPage.getRecords();
        if (records!=null){
            for (ProjectOrder order:records){
                ProjectOrderResponse response = findOrderDetail(order.getId());
                list.add(response);
            }
        }
        page.setRecords(list);
        page.setTotal(list.size());
        return page;
    }


    private void buildDtlDyAtt(StringBuffer dyAtt, JSONArray jsonArray) {
        if(!ObjectUtils.isEmpty(jsonArray)&&jsonArray.size()>0){
            jsonArray.forEach(opt -> {
                JSONObject jsonObject = (JSONObject) opt;
                String lable = jsonObject.getString("lable");
                String value = jsonObject.getString("value");
                dyAtt.append(lable).append(":").append(value);
            });
        }
    }

    private static byte[] getContentBytes(String content, String charset) {
        if (charset == null || "".equals(charset)) {
            return content.getBytes();
        }
        try {
            return content.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("MD5签名过程中出现错误,指定的编码集不对,您目前指定的编码集是:" + charset);
        }
    }

    /**
     * 校验表单重复提交redis分布式锁实现
     * @param
     */
    @Override
    public void checkRepeatedSubmitOrderRequest(
            @RequestParam("orderId") String orderId
            , @RequestParam("prefix") String prefix
            , @RequestParam("projectId") Long projectId) {
        /*//生成签名结果
        String lockKey = DigestUtils.md5Hex(getContentBytes(JSON.toJSONString(request), "UTF-8"));
        //核保防止同一时间多次点击
        String key = "order_id" + lockKey;
        boolean flag = redisTemplate.getConnectionFactory().getConnection().setNX(key.getBytes(), lockKey.getBytes());
        if (!flag) {
            throw new BusinessException("xxxxx");
        }
        redisTemplate.expire(key, 2, TimeUnit.SECONDS);

        try{

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            redisTemplate.delete(key);
        }*/

        //生MD5成签名结果
        //String lockKey = DigestUtils.md5Hex(getContentBytes(JSON.toJSONString(request), "UTF-8"));

        //核保防止同一时间多次点击
       /* String key = prefix + orderId + projectId;
        String value = UUID.randomUUID().toString();
        try {
            final boolean success = redisLockHelper.lock(key, value, 30, TimeUnit.SECONDS);
            if (!success) {
                throw new RuntimeException("重复提交,请稍后再试");
            }
            try {
                LOGGER.info("get the lock operator...");
            } catch (Throwable throwable) {
                throw new RuntimeException("系统异常");
            }
        } finally {
            //如果演示的话需要注释该代码，实际应该放开
            redisLockHelper.unlock(key, value);
        }*/

        //String orderId = DigestUtils.md5Hex(getContentBytes(JSON.toJSONString(request), "UTF-8"));

        // 防重复提交锁
        /*LOGGER.info("KEY:" + prefix + "_" + orderId + "_" + projectId);
        RLock lock = redisson.getLock(prefix + "_" + orderId + "_" + projectId);
        boolean res = false;

        try {
            // 尝试加锁，最多等待100秒，上锁以后10秒自动解锁
            res = lock.tryLock(0, 30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new BusinessException("系统异常，请联系系统管理员处理,业务ID:【" + projectId +"】");
        }
        if (!res){
            throw new BusinessException("操作太频繁,请稍后再试!");
        }*/




        // 防止正在请求过程再发起请求
        /*boolean isBack = false;
        RLock lock2 = redisson.getLock(orderId + "LOCK_BUTTON" + userId);
        lock2.lock(10,TimeUnit.SECONDS);
        if (redisClient.exists("BUTTON_PAY_")){
            isBack = true;
        }else{
            redisClient.setObject("BUTTON_PAY_"+ userId,borrowId,20*60);
        }
        lock2.unlock();
        if (isBack){
            logger.info("系统提交已经开始");
            throw new BusinessException("你有请求正在处理，请20分钟后再试！");
        }*/
    }

    @Override
    //@OrderNeedManager
    public void validateProjectOrderRequest(@RequestBody ProjectOrderRequest request) {
        if (request.getProjectId() == null || request.getProjectId() <= 0) {
            throw new InvalidParameterException("项目id不能为空");
        }
        if (request.getOrderDetailList() == null || request.getOrderDetailList().size() < 1) {
            throw new InvalidParameterException("申请单明细列不能为空");
        }

        //防止重复提交校验
        String orderId = DigestUtils.md5Hex(getContentBytes(JSON.toJSONString(request), "UTF-8"));
        checkRepeatedSubmitOrderRequest(orderId,PmsRedisPrefix.ORDER_PREFIX,request.getProjectId());

        //校验是否已经发起结算
        settlementService.checkIsSettlement(request.getProjectId());

        //校验对应的材料是否收款

        checkIncreaseMaterialFundReceive(request.getOrderDetailList());

        // 开工后退单不允许下单
        checkStartBackSingleProcess(request);


        ProjectInfo projectInfo = projectInfoRepository.selectById(request.getProjectId());

        if(ProjectCustomerType.CUS_TIANMAO.equals(projectInfo.getCustomerType())){
            infoService.checkTianMaoCustomerStageFundOver(projectInfo.getId(),DictionaryConstants.AFTER_STARTING_PAYMENT_STEP_ONE);
        }

        if(ProjectType.HARD.equals(projectInfo.getType())){
            //s实际开工日期后15天才允许下单
            long day = DateUtil.daysBetween(new Date(),projectInfo.getActualStartDate());

            if(day <=15){
                throw new BusinessException("集团临时通知：实际开工日期后15天才允许下单");
            }
        }



        for (ProjectOrderDetailResquest detail : request.getOrderDetailList()) {
            if (detail.getProjectMaterialId() == null || detail.getProjectMaterialId() <= 0) {
                throw new InvalidParameterException("主材ID不能为空");
            }
            if (ObjectUtils.isEmpty(detail.getMaterialCode())) {
                throw new InvalidParameterException("物料ID不能为空");
            }

            BigDecimal orderNum = StringUtils.isNotBlank(detail.getOrderConvertNum()) ? new BigDecimal(detail.getOrderConvertNum()) : new BigDecimal(0);
            if (orderNum.compareTo(BigDecimal.ZERO) < 1) {
                throw new InvalidParameterException("材料[" + detail.getMaterialName() + "]的下单数必须大于0");
            }

            //infoService.checkProjectGiftOrderAllowOrder(request.getProjectId(),detail.getMaterialCode());
        }

        if(!ProjectStatus.pUnderConstruction.equals(projectInfo.getProjectStatus()) && !ProjectStatus.pReady.equals(projectInfo.getProjectStatus())){
            throw new BusinessException("项目必须是在建或者准备期才可以操作下单!");
        }

        if(CabinetType.CABINET.equals(request.getMaterialTypeCode())){
            List<ElecContractAttach> contractAttachList = elecContractAttachFeign.findInfo(projectInfo.getTrackId(),BidElectronicContractType.CABINET_CONTRACT.getCode());
            if(ObjectUtils.isEmpty(contractAttachList)){
                throw new BusinessException("橱柜图纸不存在，不能下单!合同号：[" + projectInfo.getContractCode() + "]");
            }

        }else if(CabinetType.WARDROBE.equals(request.getMaterialTypeCode())){
            List<ElecContractAttach> contractAttachList = elecContractAttachFeign.findInfo(projectInfo.getTrackId(),BidElectronicContractType.WARDROBE_CONTRACT.getCode());
            if(ObjectUtils.isEmpty(contractAttachList)){
                throw new BusinessException("衣柜图纸不存在，不能下单!合同号：[" + projectInfo.getContractCode() + "]");
            }

        }
        //针对贷款客户，项目经理在下单的时候如果该客户开工款未缴纳完不允许下单，提示：开工款未缴够不允许下单
        if(!receiptApplyClient.checkReceiptApplyCompleted(projectInfo.getTrackId(),DictionaryConstants.STARTING_PAYMENT)){
            throw new BusinessException("开工款未缴够不允许下单!");
        }
    }

    public void checkStartBackSingleProcess(@RequestBody ProjectOrderRequest request) {
        EntityWrapper<ConstructionInfo> entityWrapper = new EntityWrapper<>();
        entityWrapper.where("is_deleted = 0 and START_BACK_ID in( select id from start_back_single where is_deleted = 0 and project_id= " +request.getProjectId() + ")");
        List<ConstructionInfo> constructionInfoList = constructionInfoRepository.selectList(entityWrapper);
        if(UtilTool.isNotNull(constructionInfoList)){
            throw new BusinessException("开工后退单流程发起后不允许在下订单【" + request.getProjectId() + "】");
        }
    }

    @Override
    public void validateAllMaterialsMeasured(@RequestBody ProjectOrderRequest request) {
        List<ProjectMaterial> projectMaterialResponseList = materialRepository.selectProjectMaterialListByProjectId(request.getMaterialTypeCode(), request.getProjectId(), "1");
        if (!ObjectUtils.isEmpty(projectMaterialResponseList)) {
            //根据物料类别查询是否存在关系数据
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());

            //List<String> taskMeterMaterialTypes = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrList(request.getMaterialTypeCode(), PmsTaskType.MEASUREMENT.getCode());
            for (ProjectMaterial materialResponse : projectMaterialResponseList) {
                if (taskMeterMaterialTypes.contains(materialResponse.getMaterialCode())) {
                    // TODO: 2018/8/9 暂时屏蔽
                    //throw new BusinessException("该类别下还存在未测量的材料!");
                }
            }
        }
    }

    private BigDecimal getAlreadyAmount(String code,List<SpecialAmountVo> amounts){
        if(amounts!=null&&amounts.size()>0){
            for(SpecialAmountVo amountVo:amounts){
                if(code.equals(amountVo.getSpecialCode())){
                    return amountVo.getAmount()!=null?amountVo.getAmount():new BigDecimal(0);
                }
            }
        }
        return new BigDecimal(0);
    }

    @Override
    public PmProjectOrderCheckExcessDto validateOrderExcess(@RequestBody ProjectOrderRequestVo param) {
        LOGGER.error("######[validateOrderExcess.request]参数【" + JSON.toJSONString(param)+"]######");

        PmProjectOrderCheckExcessDto excessDto = new PmProjectOrderCheckExcessDto();
        excessDto.setStatus(true);
        //获取项目信息
        ProjectInfo projectInfo = projectInfoRepository.selectById(param.getProjectId());

        if(!ProjectStatus.pUnderConstruction.equals(projectInfo.getProjectStatus()) && !ProjectStatus.pReady.equals(projectInfo.getProjectStatus())){
            throw new BusinessException("项目必须是在建或者准备期才可以操作下单!");
        }

        //校验是否已经发起结算
        settlementService.checkIsSettlement(param.getProjectId());

        if(!ProjectType.SOFT.equals(projectInfo.getType())){
            CostData costData = new CostData();
            costData.setIsDeleted(0);
            costData.setBizOpId(projectInfo.getTrackId());
            costData.setAccountType(DictionaryConstants.ACCOUNT_TYPE_SEND_PACK);
            costData.setState(DictionaryConstants.COST_COMPLETE);
            List<CostData> costDataList = tamsCostFeign.list(costData);

            if (ObjectUtils.isEmpty(costDataList)) {
                LOGGER.error("######[ProjectOrderServiceImpl.addProjectOrder]发包查询失败：项目经理发包表为空【" + projectInfo.getContractCode()+"]######");
                throw new BusinessException("项目经理未确认接包！商机ID[" + projectInfo.getTrackId() +"]");
            }
        }

        //BigDecimal cost = new BigDecimal(param.getCost());

        if(true){
            ProjectPaymentVo paymentVo = new ProjectPaymentVo();

            //支付物料明细
            List<ProjectExpensePaymentDetail> paymentDetails=Lists.newArrayList();

            //获取成本控制信息
            CostControlCalcRequest calcRequest = new CostControlCalcRequest();
            calcRequest.setBizOpId(projectInfo.getTrackId());
            calcRequest.setCode("");
            List<ControlConfigVoResponse> controlList = costControlConfigFeign.calc(calcRequest);
            //便利成本控制将当前所有物料进行归类

            //实际须支付金额
            BigDecimal readyPayAmount = BigDecimal.ZERO;

            //计算依据支付的金额
            List<String> charNos=Lists.newArrayList();
            //获取已经交付的金额
            List<SpecialAmountVo> amounts = expensePaymentRepository.statisticsAlreadyAmount(param.getProjectId(),"ORDER",charNos);

            for (ControlConfigVoResponse response : controlList) {
                //已交付金额
                BigDecimal alreadyAmount = getAlreadyAmount(response.getCode(),amounts);
                //已下单金额
                BigDecimal orderedAmount = response.getOrderAmount();
                //计算本次应交金额
                BigDecimal thisAmount = BigDecimal.ZERO;
                for(ProjectOrderDetailResquest detailResquest : param.getOrderDetailList()){
                    ProjectMaterial material = materialRepository.selectById(detailResquest.getProjectMaterialId());
                    BigDecimal pmPrice = new BigDecimal(detailResquest.getProManagerPrice());
                    BigDecimal materialNum = new BigDecimal(detailResquest.getOrderNum());
                    String specialCode = material.getSpecialCode();
                    String includeCodeStr = response.getIncludeCode();
                    String excludeCodeStr = response.getExcludeCode();
                    if (ObjectUtils.isEmpty(includeCodeStr) || includeCodeStr.length() == 0) {
                        continue;
                    }
                    // 白名单
                    Set<String> includeCodes = new HashSet<>();
                    // 黑名单
                    Set<String> excludeCodes = new HashSet<>();
                    CollectionUtils.addAll(includeCodes, includeCodeStr.replaceAll("，", ",").split(","));
                    if (!ObjectUtils.isEmpty(excludeCodeStr) && includeCodeStr.length() > 0)
                        CollectionUtils.addAll(excludeCodes, excludeCodeStr.replaceAll("，", ",").split(","));
                    boolean jumpFlag = false;
                    for (String code : includeCodes) {
                        // 如果在黑名单中， 就不考虑了。
                        if (excludeCodes.contains(specialCode)) {
                            continue;
                        }
                        if (code.contains("%") || code.contains("％")) {
                            String formulaParam = code.replaceAll("%", Matcher.quoteReplacement("\\w*")).replaceAll("％", Matcher.quoteReplacement("\\w*"));
                            //LOGGER.info("######[ProjectOrderServiceImpl.validateExcessForDecrease]formulaParam：[" + formulaParam + "],特项代码[" + specialCode + "]######");
                            if(ObjectUtils.isEmpty(specialCode)){
                                //LOGGER.error("######[ProjectOrderServiceImpl.validateExcessForDecrease]formulaParam：[" + formulaParam + "],特项代码[" + specialCode + "]######");
                                throw new BusinessException("特项代码不能为空，sku:[" + detailResquest.getMaterialCode()+"]");
                            }
                            boolean isExist = specialCode.matches(formulaParam);
                            if (isExist) {
                                LOGGER.info("thisAmunt:[" + thisAmount.stripTrailingZeros().toPlainString() + "]");
                                LOGGER.info("pmPrice:[" + pmPrice.stripTrailingZeros().toPlainString() + "]");
                                LOGGER.info("materialNum:[" + materialNum.stripTrailingZeros().toPlainString() + "]");
                                thisAmount = thisAmount.add(pmPrice.multiply(materialNum).setScale(2,BigDecimal.ROUND_DOWN));
                                LOGGER.info("thisAmunt+:[" + thisAmount.stripTrailingZeros().toPlainString() + "]");
                                jumpFlag = true;
                                break;
                            }
                        }
                        if (specialCode.equals(code)) {
                            LOGGER.info("thisAmunt:[" + thisAmount.stripTrailingZeros().toPlainString() + "]");
                            LOGGER.info("pmPrice:[" + pmPrice.stripTrailingZeros().toPlainString() + "]");
                            LOGGER.info("materialNum:[" + materialNum.stripTrailingZeros().toPlainString() + "]");
                            thisAmount = thisAmount.add(pmPrice.multiply(materialNum).setScale(2,BigDecimal.ROUND_DOWN));
                            LOGGER.info("thisAmunt+:[" + thisAmount.stripTrailingZeros().toPlainString() + "]");
                            jumpFlag = true;
                            break;
                        }
                    }
                    if (jumpFlag) {
                       // break;
                    }
                }
                if(thisAmount.compareTo(BigDecimal.ZERO) <=0){
                    continue;
                }
                LOGGER.info("orderedAmount+:[" + orderedAmount.stripTrailingZeros().toPlainString() + "]");
                LOGGER.info("getMaxAmount+:[" + response.getMaxAmount().stripTrailingZeros().toPlainString() + "]");
                LOGGER.info("alreadyAmount+:[" + alreadyAmount.stripTrailingZeros().toPlainString() + "]");
                BigDecimal thisPayAmount = thisAmount.add(orderedAmount).subtract(response.getMaxAmount()).subtract(alreadyAmount);

                LOGGER.info("thisPayAmount+:[" + thisPayAmount.stripTrailingZeros().toPlainString() + "]");

                if(thisPayAmount.compareTo(BigDecimal.ZERO) <=0){
                    continue;
                }

                if(thisPayAmount.compareTo(new BigDecimal("0.1")) <=0){
                    continue;
                }

                readyPayAmount = readyPayAmount.add(thisPayAmount);

                LOGGER.info("readyPayAmount+:[" + readyPayAmount.stripTrailingZeros().toPlainString() + "]");

                ProjectExpensePaymentDetail paymentDetail = new ProjectExpensePaymentDetail();
                paymentDetail.setTypeName(response.getName());
                paymentDetail.setTypeCode(response.getCode());
                paymentDetail.setPaymentId(null);
                paymentDetail.setPayPrice(new BigDecimal(0));
                paymentDetail.setTotalPrice(new BigDecimal(0));
                paymentDetail.setMaterialNum(BigDecimal.ONE);
                paymentDetail.setMaterialPrice(thisPayAmount);
                paymentDetail.fillOperationInfo(param.getEmployeeCode());
                paymentDetails.add(paymentDetail);
            }

            if(readyPayAmount.compareTo(BigDecimal.ZERO) > 0) {

                //创建支付信息
                ProjectExpensePayment payment = new ProjectExpensePayment();
                payment.setPaymentCode("PC" + DateUtil.date2Str(new Date(), ("yyyyMMddHHmmssSSS")));//编号
                payment.setPaymentAccountCode(param.getEmployeeCode());
                payment.setPaymentAccountName(param.getEmployeeName());
                payment.setPaymentCost(readyPayAmount.setScale(2, BigDecimal.ROUND_DOWN));
                payment.setPaymentName(DateUtil.date2Str(new Date(), "yyyyMMdd") + "下单材料超支费用");
                payment.setProjectId(param.getProjectId());
                payment.fillOperationInfo(param.getEmployeeCode());
                //创建支付与原订单对应关系
                ProjectExpensePaymentSource paymentSource = new ProjectExpensePaymentSource();
                paymentSource.setBizType("project_order");
                paymentSource.setRequestData(JSON.toJSONString(param));
                paymentSource.fillOperationInfo(param.getEmployeeCode());

                paymentVo.setPayment(payment);
                paymentVo.setPaymentDetailList(paymentDetails);
                paymentVo.setPaymentSource(paymentSource);
                payment = paymentService.addPayment(paymentVo);
                //提交到支付服务

                JSONObject params = new JSONObject();
                params.put("goodsId", payment.getId());
                params.put("goodsNum", "1");
                params.put("accountID", 1);
                params.put("accountName", payment.getPaymentAccountName());
                if (true) {
                    params.put("price", payment.getPaymentCost().toString());
                }
                //非生产环境,默认支付1分钱
                else {
                    params.put("price", "0.01");
                }
                params.put("goodsName", payment.getPaymentName());
                params.put("notifyUrl", paymentNotifyUrl);
                LOGGER.info("######[ProjectOrderServiceImpl.validateOrderExcess] 请求支付服务notifyUrl：" + paymentUrl + "######");
                LOGGER.info("######[ProjectOrderServiceImpl.validateOrderExcess] 请求支付服务notifyUrl：" + paymentNotifyUrl + "######");

                String result = null;
                try {
                    LOGGER.info("######[ProjectOrderServiceImpl.validateOrderExcess] 请求支付服务param：" + params.toJSONString() + "######");

                    result = HttpClientUtils.httpPost(paymentUrl, params.toString());

                    LOGGER.info("######[ProjectOrderServiceImpl.validateOrderExcess] 请求支付服务jresult：" + result + "######");

                } catch (Exception e) {
                    e.printStackTrace();

                    LOGGER.error("######[ProjectOrderServiceImpl.validateOrderExcess] 请求支付服务失败：" + e.getMessage() + "######");
                    throw new BusinessException("请求支付服务失败：" + e.getMessage());
                }

                JSONObject jsonObject = JSONObject.parseObject(StringUtils.isNotBlank(result) ? result : "{}");
                if (!jsonObject.containsKey("code") || jsonObject.getInteger("code") != 0) {

                    LOGGER.error("######[ProjectOrderServiceImpl.validateOrderExcess] 请求支付服务失败：" + (jsonObject.containsKey("message") ? jsonObject.getString("message") : result) + "######");
                    throw new BusinessException("请求支付服务失败：" + (jsonObject.containsKey("message") ? jsonObject.getString("message") : result));
                }
                //回写支付流水编号至主表中
                ProjectExpensePayment update = new ProjectExpensePayment();
                update.setId(payment.getId());
                update.setSourceType(PaymentSourceType.ORDER.getType());
                update.setOrderWaterNo(jsonObject.getString("orderWaterNo"));
                update.fillOperationInfo(param.getEmployeeCode());
                paymentService.saveOrUpdate(update);
                //响应
                excessDto.setStatus(false);
                excessDto.setMessage("您本次下单材料费用已超出,需支付[" + payment.getPaymentCost().toString() + "]元才能完成下单,确认要支付吗?");

                LOGGER.info("######[ProjectOrderServiceImpl.validateOrderExcess] 您本次下单材料费用已超出,需支付[" + payment.getPaymentCost().toString() + "]元才能完成下单,确认要支付吗?######");

                //object.put("message","您本次下单材料费用已超出,需支付["+payment.getPaymentCost().toString()+"]元才能完成下单,确认要支付吗?");
                PaymentDto detail = new PaymentDto();
                detail.setPaymentId(payment.getId());
                detail.setPaymentData(jsonObject.getString("data"));
                excessDto.setDetail(detail);
            }
        }
        return excessDto;
    }

    @Override
    public PmProjectOrderCheckExcessDto validateSaleCourseOrderExcess(@RequestBody ProjectSaleCourseOrderRequest request) {

        LOGGER.error("######[validateSaleCourseOrderExcess.request]参数【" + JSON.toJSONString(request)+"]######");

        //防止表单重复提交在30秒内
        String orderId = DigestUtils.md5Hex(getContentBytes(JSON.toJSONString(request), "UTF-8"));
        checkRepeatedSubmitOrderRequest(orderId,PmsRedisPrefix.SALE_ORDER_PREFIX,request.getSaleId());

        PmProjectOrderCheckExcessDto excessDto = new PmProjectOrderCheckExcessDto();
        excessDto.setStatus(true);
        SaleCourseManage saleCourseManage = manageRepository.selectById(request.getSaleId());

       // List<ProjectOrderDetail> orderDetail = orderDetailRepository.selectOrderDetailsByOmsOrderCode(saleCourseManage.getOrderCode());

        //Long projectId = orderDetail.get(0).getProjectId();

        //获取项目信息
        ProjectInfo projectInfo = projectInfoRepository.selectByContractCode(saleCourseManage.getProjectCode());
        if(UtilTool.isNull(projectInfo)){
            throw new BusinessException("项目不存在!");
        }

        //在建校验
        if(!projectInfo.getProjectStatus().equals(ProjectStatus.pUnderConstruction)){
            throw new BusinessException("项目必须是在建状态才可以操作!");
        }
        //驳回
        if(false){
            //查询当前节点
            ValidateFlowIsExistRequest requestV=new ValidateFlowIsExistRequest();
            requestV.setBussId(saleCourseManage.getId().toString());//entity.getCode()
            requestV.setKeyField("id");
            requestV.setFlowKeyList(Arrays.asList(DictionaryConstants.SALE_COURSE_PROCESS));
            List<CurrentNodeResponse> taskInfo = bpmFeign.getRunTaskInfo(requestV);
            if(taskInfo==null||taskInfo.isEmpty()){
                throw new InvalidParameterException("查询无此流程节点");
            }else{
                //发起流程
                CurrentNodeResponse currentNodeResponse = taskInfo.get(0);
                List<String> auditorList = currentNodeResponse.getAuditorList();
                if(!auditorList.contains(saleCourseManage.getVendorCode())){
                    //throw new InvalidParameterException("节点审批人和查询出的审批人不一致");
                }

                FlowCheckRequest<Object> flowCheckRequest=new FlowCheckRequest<>();
                flowCheckRequest.setTaskId(currentNodeResponse.getTaskId());
                flowCheckRequest.setFlowKey(DictionaryConstants.SALE_COURSE_PROCESS);
                flowCheckRequest.setState("2");
                //flowCheckRequest.setAuditor(profileServiceClient.findProfileByWorkNumber(saleCourseManageDb.getVendorCode(), AppProperties.PMS));
                // TODO: 2019/3/19 包装流程中项目经理审核人信息
                ProjectInfo info = projectInfoRepository.selectByContractCode(saleCourseManage.getProjectCode());
                UnifiedProfile unifiedProfile = new UnifiedProfile();
                unifiedProfile.setWorkNumber(WorkNumberUtil.getLaborWorkNumber(info.getProjectManagerCode(),info.getProjectManagerName()));
                flowCheckRequest.setAuditor(unifiedProfile);
                Map<String, Object> map = new HashMap<>();
                map.put("state", "2");
                flowCheckRequest.setVariableMap(map);
                flowCheckRequest.setAuditRemark(request.getRemark());
                bpmFeign.checkProcess(flowCheckRequest);
            }
            return null;
        }

        //校验是否已经发起结算
        settlementService.checkIsSettlement(projectInfo.getId());
        BigDecimal cost = saleCourseManage.getProjectManagerAmount()==null?BigDecimal.ZERO:saleCourseManage.getProjectManagerAmount();
        if(cost.compareTo(new BigDecimal(0)) >=0){
            ProjectPaymentVo paymentVo = new ProjectPaymentVo();

            //支付物料明细
            List<ProjectExpensePaymentDetail> paymentDetails=Lists.newArrayList();
            //待支付金额
            BigDecimal actualPayAmount = BigDecimal.ZERO;
            //查询已支付金额
            List<ProjectExpensePayment> paymentList = paymentRepository.getSuccessList(projectInfo.getId(),request.getSaleId(),"SALE_COURSE");
            if(UtilTool.isNotNull(paymentList)){
                for(ProjectExpensePayment expensePayment : paymentList){
                    actualPayAmount = actualPayAmount.add(expensePayment.getPaymentCost());
                }
            }
            BigDecimal readyPayAmount = cost.subtract(actualPayAmount);//saleCourseManageService.getTotalAmount(request.getSaleId());

            ProjectExpensePaymentDetail paymentDetail = new ProjectExpensePaymentDetail();
            paymentDetail.setTypeName("售中售后补件费用");
            paymentDetail.setTypeCode(saleCourseManage.getCode());
            paymentDetail.setPaymentId(null);
            paymentDetail.setPayPrice(new BigDecimal(0));
            paymentDetail.setTotalPrice(new BigDecimal(0));
            paymentDetail.setMaterialNum(BigDecimal.ONE);
            paymentDetail.setMaterialPrice(readyPayAmount);
            paymentDetail.fillOperationInfo(request.getEmployeeCode());
            paymentDetails.add(paymentDetail);

            LOGGER.info("readyPayAmount+:[" + readyPayAmount.stripTrailingZeros().toPlainString() + "]");

            if(readyPayAmount.compareTo(new BigDecimal(0)) > 0) {
                //创建支付信息
                ProjectExpensePayment payment = new ProjectExpensePayment();
                payment.setPaymentCode("PC" + DateUtil.date2Str(new Date(), ("yyyyMMddHHmmssSSS")));//编号
                payment.setPaymentAccountCode(request.getCost());
                payment.setPaymentAccountName(request.getEmployeeName());
                payment.setPaymentCost(readyPayAmount.setScale(2, BigDecimal.ROUND_HALF_UP));
                payment.setPaymentName(DateUtil.date2Str(new Date(), "yyyyMMdd") + "下单材料超支费用");
                payment.setProjectId(projectInfo.getId());
                payment.fillOperationInfo(request.getEmployeeCode());
                //创建支付与原订单对应关系
                ProjectExpensePaymentSource paymentSource = new ProjectExpensePaymentSource();
                paymentSource.setBizType("project_sale_course_order");
                paymentSource.setBizId(request.getSaleId());
                paymentSource.setRequestData(JSON.toJSONString(request));
                paymentSource.fillOperationInfo(request.getEmployeeCode());

                paymentVo.setPayment(payment);
                paymentVo.setPaymentDetailList(paymentDetails);
                paymentVo.setPaymentSource(paymentSource);
                payment = paymentService.addPayment(paymentVo);
                //提交到支付服务
                JSONObject params = new JSONObject();
                params.put("goodsId", payment.getId());
                params.put("goodsNum", "1");
                params.put("accountID", 1);
                params.put("accountName", payment.getPaymentAccountName());
                if (true) {
                    params.put("price", payment.getPaymentCost().toString());
                }
                //非生产环境,默认支付1分钱
                else {
                    params.put("price", "0.01");
                }
                params.put("goodsName", payment.getPaymentName());
                params.put("notifyUrl", paymentNotifyUrl);
                LOGGER.info("######[ProjectOrderServiceImpl.validateSaleCourseOrderExcess] 请求支付服务notifyUrl：" + paymentUrl + "######");
                LOGGER.info("######[ProjectOrderServiceImpl.validateSaleCourseOrderExcess] 请求支付服务notifyUrl：" + paymentNotifyUrl + "######");

                String result = null;
                try {
                    LOGGER.info("######[ProjectOrderServiceImpl.validateSaleCourseOrderExcess] 请求支付服务param：" + params.toJSONString() + "######");

                    result = HttpClientUtils.httpPost(paymentUrl, params.toString());

                    LOGGER.info("######[ProjectOrderServiceImpl.validateSaleCourseOrderExcess] 请求支付服务jresult：" + result + "######");

                } catch (Exception e) {
                    e.printStackTrace();

                    LOGGER.error("######[ProjectOrderServiceImpl.validateSaleCourseOrderExcess] 请求支付服务失败：" + e.getMessage() + "######");
                    throw new BusinessException("请求支付服务失败：" + e.getMessage());
                }

                JSONObject jsonObject = JSONObject.parseObject(StringUtils.isNotBlank(result) ? result : "{}");
                if (!jsonObject.containsKey("code") || jsonObject.getInteger("code") != 0) {
                    LOGGER.error("######[ProjectOrderServiceImpl.validateSaleCourseOrderExcess] 请求支付服务失败：" + (jsonObject.containsKey("message") ? jsonObject.getString("message") : result) + "######");
                    throw new BusinessException("请求支付服务失败：" + (jsonObject.containsKey("message") ? jsonObject.getString("message") : result));
                }
                //回写支付流水编号至主表中
                ProjectExpensePayment update = new ProjectExpensePayment();
                update.setId(payment.getId());
                update.setSourceType(PaymentSourceType.SALE_COURSE.getType());
                update.setOrderWaterNo(jsonObject.getString("orderWaterNo"));
                update.fillOperationInfo(request.getEmployeeCode());
                paymentService.saveOrUpdate(update);
                //响应
                excessDto.setStatus(false);
                excessDto.setMessage("您本次下单售中补件材料,需支付[" + payment.getPaymentCost().toString() + "]元才能完成下单,确认要支付吗?");

                LOGGER.info("######[ProjectOrderServiceImpl.validateSaleCourseOrderExcess] 您本次下单材料费用已超出,需支付[" + payment.getPaymentCost().toString() + "]元才能完成下单,确认要支付吗?######");

                PaymentDto detail = new PaymentDto();
                detail.setPaymentId(payment.getId());
                detail.setPaymentData(jsonObject.getString("data"));
                excessDto.setDetail(detail);
            }
        }
        return excessDto;
    }

    @Override
    @Transactional
    public void updateOrderStatus(@RequestBody PmsOrderStateResponse orderReturnVo) {
        //1.插入日志
        LOGGER.info("######订单[" + orderReturnVo.getSourceCode() + "] 开始更新状态######");

        //ProjectInfo info = projectInfoRepository.selectByContractCode(orderReturnVo.getProjectCode());
        List<PmsOrderStateDetailResponse> detailVoList = orderReturnVo.getDetails();
        ProjectOrder order = orderRepository.selectOrderByCode(orderReturnVo.getSourceCode());
        ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());
        if(!order.getStatus().equals(PmsOrderStatus.PO_SUBMITTED.getCode())){

            LOGGER.error("######[ProjectOrderServiceImpl.updateOrderStatus]订单状态返回处理异常：单号[" + order.getOrderCode()+"]状态是还未提交状态，不能更新状态######");
            throw new BusinessException("订单状态返回处理异常：单号[" + order.getOrderCode()+"]状态是还未提交状态，不能更新状态");
        }

        //2订单确认
        if (orderReturnVo.getState().equals("1")) {
            //订单判断是否有地材，有则生成领料单明细
            if (orderReturnVo.getType().equals(OrderType.PURCHASE)) {
                buildAutoPickingAndDetailList(orderReturnVo, info, detailVoList);
            }
            updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_ORDER_CONFIRMED, PmsMaterialTypeStatus.MT_ORDER_CONFIRMED);
        } else if (orderReturnVo.getState().equals("2")) {//入库

            updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_IN_STORAGE, PmsMaterialTypeStatus.MT_IN_STORAGE);
        } else if (orderReturnVo.getState().equals("3")) {//发货确认
            //如果是地才的则更新为picked
            updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_ORDER_SEND_CONFIRMED, PmsMaterialTypeStatus.MT_ORDER_SEND_CONFIRMED);


        } else {//配送出库 4

            updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_DELIVERED, PmsMaterialTypeStatus.MT_DELIVERED);

            // 是否是地材的 更新领料单明细状态
            buildOrderChainInfo(orderReturnVo, info, order);

        }

        LOGGER.info("######[ProjectOrderServiceImpl.updateOrderStatus]订单[" + orderReturnVo.getSourceCode() + "] 开始更新状态完毕######");
    }

    @Override
    @Transactional
    public void updateOrderStatusByPmsOrder(@RequestBody List<Long> ids) {
        if(ObjectUtils.isEmpty(ids)){
            return;
        }
        for(Long id : ids){
            ProjectOrder order = orderRepository.selectById(id);
            ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());
            List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(id);
            PmsOrderStateResponse orderReturnVo = new PmsOrderStateResponse();
            List<PmsOrderStateDetailResponse> details = Lists.newArrayList();
            orderReturnVo.setState("4");
            orderReturnVo.setProjectCode(info.getProjectCode());
            orderReturnVo.setSourceCode(order.getOrderCode());
            orderReturnVo.setOperator("admin");
            orderReturnVo.setOperatorName("系统管理员");
            orderReturnVo.setOperateTime(new Date());
            orderReturnVo.setType(OrderType.PURCHASE);
            orderReturnVo.setRemark("补偿");
            for(ProjectOrderDetail orderDetail : orderDetailList){
                PmsOrderStateDetailResponse stateDetailResponse = new PmsOrderStateDetailResponse();
                stateDetailResponse.setLineNo(orderDetail.getItemId());
                stateDetailResponse.setBillCode(orderDetail.getOmsOrderCode());
                details.add(stateDetailResponse);
            }
            orderReturnVo.setDetails(details);

            pickingService.checkSoftStartWorkProcessByOrderState(orderReturnVo,PmsMaterialStatus.M_IN_STORAGE.getCode());

            //1.插入日志
            LOGGER.info("######订单[" + orderReturnVo.getSourceCode() + "] 开始更新状态######");

            List<PmsOrderStateDetailResponse> detailVoList = orderReturnVo.getDetails();

            if(!order.getStatus().equals(PmsOrderStatus.PO_SUBMITTED.getCode())){

                LOGGER.error("######[ProjectOrderServiceImpl.updateOrderStatus]订单状态返回处理异常：单号[" + order.getOrderCode()+"]状态是还未提交状态，不能更新状态######");
                throw new BusinessException("订单状态返回处理异常：单号[" + order.getOrderCode()+"]状态是还未提交状态，不能更新状态");
            }

            //2订单确认
            if (orderReturnVo.getState().equals("1")) {
                //订单判断是否有地材，有则生成领料单明细
                if (orderReturnVo.getType().equals(OrderType.PURCHASE)) {
                    buildAutoPickingAndDetailList(orderReturnVo, info, detailVoList);
                }
                updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_ORDER_CONFIRMED, PmsMaterialTypeStatus.MT_ORDER_CONFIRMED);
            } else if (orderReturnVo.getState().equals("2")) {//入库

                updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_IN_STORAGE, PmsMaterialTypeStatus.MT_IN_STORAGE);
            } else if (orderReturnVo.getState().equals("3")) {//发货确认
                //如果是地才的则更新为picked
                updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_ORDER_SEND_CONFIRMED, PmsMaterialTypeStatus.MT_ORDER_SEND_CONFIRMED);


            } else {//配送出库

                updateProjectMaterialStatus(orderReturnVo, info, detailVoList, PmsMaterialStatus.M_DELIVERED, PmsMaterialTypeStatus.MT_DELIVERED);

                // 是否是地材的 更新领料单明细状态
                buildOrderChainInfo(orderReturnVo, info, order);

            }
            LOGGER.info("######[ProjectOrderServiceImpl.updateOrderStatus]订单[" + orderReturnVo.getSourceCode() + "] 开始更新状态完毕######");
        }

    }



    /**
     * 创建售中售后订单
     */
    @Override
    @Transactional
    public void updateSaleCourceOrderStatus(@RequestBody BatchOrRepairOrderResponse orderResponse) {
        if(ObjectUtils.isEmpty(orderResponse.getSourceCode())){
            throw new BusinessException("业务单号不能为空");
        }
        EntityWrapper<SaleCourseManage> scEw = new EntityWrapper<>();
        SaleCourseManage saleCourseManage = new SaleCourseManage();
        saleCourseManage.setCode(orderResponse.getSourceCode());
        saleCourseManage.setIsDeleted(0);
        scEw.setEntity(saleCourseManage);
        SaleCourseManage saleCourseManageDb = manageRepository.selectOne(scEw);
        if(ObjectUtils.isEmpty(saleCourseManage)){
            LOGGER.info("######[ProjectOrderServiceImpl.createSaleCourceOrder]订单[" + orderResponse.getSourceCode() + "] 业务对象未查询到######");
            throw new BusinessException("未查到售中业务单：【" + orderResponse.getSourceCode() + "】");
        }

        OrderInfo entity = new OrderInfo();
        entity.setSourceCode(orderResponse.getSourceCode());
        entity.setIsDeleted(0);
        List<OrderInfo> orderInfoList = orderInfoFeign.list(entity);
        if(ObjectUtils.isEmpty(orderInfoList)){
            LOGGER.info("######[ProjectOrderServiceImpl.createSaleCourceOrder]订单[" + orderResponse.getSourceCode() + "] OMS订单未查询到######");
            throw new BusinessException("未查询到对应的订单信息：【" + orderResponse.getSourceCode() + "】");
        }

        if("0".equals(orderResponse.getState())){
            createSaleCourseOrderByOmsOrder(orderResponse, saleCourseManageDb);
            return;
        }

        OrderInfo orderInfo = orderInfoList.get(0);
        EntityWrapper<ProjectOrder> entityWrapper = new EntityWrapper<>();
        entityWrapper.where("source_order = '" + orderInfo.getCode() + "'");
        ProjectOrder projectOrder = orderRepository.selectOne(entityWrapper);
        if(ObjectUtils.isEmpty(projectOrder)){
            LOGGER.info("######[ProjectOrderServiceImpl.createSaleCourceOrder]订单[" + orderResponse.getSourceCode() + "] 交付订单未查询到######");
            throw new BusinessException("未查询到对应的订单信息：【" + orderResponse.getSourceCode() + "】");
        }

        //包装oms返回信息
        PmsOrderStateResponse orderReturnVo = buildOmsOrderReturnByOmsSaleCourseResponse(orderResponse, orderInfo, projectOrder);

        if("1".equals(orderResponse.getState())){
            updateOrderStatus(orderReturnVo);
            return;
        }
        if("2".equals(orderResponse.getState())){
            updateOrderStatus(orderReturnVo);
            return;
        }
        if("3".equals(orderResponse.getState())){
            updateOrderStatus(orderReturnVo);
            return;
        }


    }

    @Override
    public void cancelOrInStoreSaleCourceOrderStatus(@RequestBody OrderSysOperateRequest orderResponse) {
        PmsOrderStateResponse orderStateResponse = buildOmsOrderReturnByOmsSaleCourseResponse(orderResponse);
        // 取消
        if("0".equals(orderStateResponse.getState())){
            if(ObjectUtils.isEmpty(orderStateResponse.getDetails())){
                throw new BusinessException("未查询对应的订单明细：【" + orderStateResponse.getSourceCode() + "】");
            }
            //查询对应的交付订单
            List<ProjectOrderDetail> orderDetailList = orderDetailRepository.selectOrderDetailsByOmsOrderCode(orderStateResponse.getDetails().get(0).getBillCode());
            if(ObjectUtils.isEmpty(orderDetailList)){
                throw new BusinessException("未查询对应的订单明细：【" + orderStateResponse.getSourceCode() + "】");
            }
            this.cancelOrderAllByOMS(orderDetailList.get(0).getOrderId(),orderStateResponse);
            return;
        }
        //入库
        if("2".equals(orderStateResponse.getState())){
            updateOrderStatus(orderStateResponse);
            return;
        }
    }

    @Override
    public List<ProjectOrder> findOrderListByProjectId(@PathVariable("id") Long id) {

        return orderRepository.selectProjectOrderListByProjectId(id);
    }

    @Override
    public List<ProjectOrder> findOrderListByBizoptId(@PathVariable("id") Long id) {

        return orderRepository.selectProjectOrderListByBizoptId(id);
    }

    private PmsOrderStateResponse buildOmsOrderReturnByOmsSaleCourseResponse(OrderSysOperateRequest orderResponse) {
        if(ObjectUtils.isEmpty(orderResponse.getCode())){
            throw new BusinessException("业务单号不能为空");
        }
        OrderInfo entity = new OrderInfo();
        entity.setSourceCode(orderResponse.getCode());
        entity.setIsDeleted(0);
        List<OrderInfo> orderInfoList = orderInfoFeign.list(entity);
        if(ObjectUtils.isEmpty(orderInfoList)){
            LOGGER.info("######[ProjectOrderServiceImpl.buildOmsOrderReturnByOmsSaleCourseResponse]订单[" + orderResponse.getCode() + "] OMS订单未查询到######");
            throw new BusinessException("未查询到对应的订单信息：【" + orderResponse.getCode() + "】");
        }
        OrderInfo orderInfo = orderInfoList.get(0);

        EntityWrapper<ProjectOrder> entityWrapper = new EntityWrapper<>();
        entityWrapper.where("source_order = '" + orderInfo.getCode() + "'");
        ProjectOrder projectOrder = orderRepository.selectOne(entityWrapper);
        if(ObjectUtils.isEmpty(projectOrder)){
            LOGGER.info("######[ProjectOrderServiceImpl.buildOmsOrderReturnByOmsSaleCourseResponse]订单[" + orderResponse.getCode() + "] 交付订单未查询到######");
            throw new BusinessException("未查询到对应的订单信息：【" + orderResponse.getCode() + "】");
        }

        PmsOrderStateResponse orderReturnVo = new PmsOrderStateResponse();
        List<PmsOrderStateDetailResponse> details = Lists.newArrayList();
        orderReturnVo.setDetails(details);
        orderReturnVo.setType(OrderType.PURCHASE);
        orderReturnVo.setOperateTime(new Date());
        orderReturnVo.setOperator(orderResponse.getWorkNumber());
        orderReturnVo.setOperatorName("");
        if(2==orderResponse.getOrderState()) { //取消
            orderReturnVo.setState("0");
        } else if(1==orderResponse.getOrderState()) { //入库
            orderReturnVo.setState("2");
        }else{

        }
        orderReturnVo.setProjectCode(orderInfo.getProjectCode());
        orderReturnVo.setSourceCode(projectOrder.getOrderCode());
        List<OrderDetail> omsOrderDetailList = orderDetailFeign.findDetailByOrderCode(orderInfo.getCode());
        for(OrderDetail orderDetail : omsOrderDetailList){
            PmsOrderStateDetailResponse detailResponse = new PmsOrderStateDetailResponse();
            detailResponse.setBillCode(orderInfo.getCode());
            detailResponse.setLineNo(orderDetail.getSourceLineNo().toString());
            details.add(detailResponse);
        }
        return orderReturnVo;
    }

    private PmsOrderStateResponse buildOmsOrderReturnByOmsSaleCourseResponse(BatchOrRepairOrderResponse orderResponse, OrderInfo orderInfo, ProjectOrder projectOrder) {
        PmsOrderStateResponse orderReturnVo = new PmsOrderStateResponse();
        List<PmsOrderStateDetailResponse> details = Lists.newArrayList();
        orderReturnVo.setDetails(details);
        orderReturnVo.setType(OrderType.PURCHASE);
        orderReturnVo.setOperateTime(orderResponse.getOperateTime());
        orderReturnVo.setOperator(orderResponse.getOperator());
        orderReturnVo.setOperatorName(orderResponse.getOperatorName());
        if("0".equals(orderResponse.getState())) {
            //orderReturnVo.setState("1");
        }else if("1".equals(orderResponse.getState())) { //订单确认
            orderReturnVo.setState("1");
        }else if("2".equals(orderResponse.getState())) { //发货确认
            orderReturnVo.setState("3");
        }
        orderReturnVo.setProjectCode(orderResponse.getProjectCode());
        orderReturnVo.setSourceCode(projectOrder.getOrderCode());
        List<OrderDetail> omsOrderDetailList = orderDetailFeign.findDetailByOrderCode(orderInfo.getCode());
        for(OrderDetail orderDetail : omsOrderDetailList){
            PmsOrderStateDetailResponse detailResponse = new PmsOrderStateDetailResponse();
            detailResponse.setBillCode(orderInfo.getCode());
            detailResponse.setLineNo(orderDetail.getSourceLineNo().toString());
            details.add(detailResponse);
        }
        return orderReturnVo;
    }

    private void updateSaleCourseOrderStatus(BatchOrRepairOrderResponse orderResponse,SaleCourseManage saleCourseManageDb){
        //1.插入申请表数据
        ProjectInfo projectInfo = projectInfoRepository.selectByContractCode(saleCourseManageDb.getProjectCode());

        OrderInfo entity = new OrderInfo();
        entity.setSourceCode(orderResponse.getSourceCode());
        entity.setIsDeleted(0);
        List<OrderInfo> orderInfoList = orderInfoFeign.list(entity);
        if(ObjectUtils.isEmpty(orderInfoList)){
            throw new BusinessException("未查询到对应的订单信息：【" + orderResponse.getSourceCode() + "】");
        }
        OrderInfo orderInfo = orderInfoList.get(0);

        //查询交付订单明细



    }


    private void createSaleCourseOrderByOmsOrder(BatchOrRepairOrderResponse orderResponse, SaleCourseManage saleCourseManageDb) {
        //1.插入申请表数据
        ProjectInfo projectInfo = projectInfoRepository.selectByContractCode(saleCourseManageDb.getProjectCode());

        OrderInfo entity = new OrderInfo();
        entity.setSourceCode(orderResponse.getSourceCode());
        entity.setIsDeleted(0);
        List<OrderInfo> orderInfoList = orderInfoFeign.list(entity);
        if(ObjectUtils.isEmpty(orderInfoList)){
            throw new BusinessException("未查询到对应的订单信息：【" + orderResponse.getSourceCode() + "】");
        }
        OrderInfo orderInfo = orderInfoList.get(0);
        List<OrderDetail> omsOrderDetailList = orderDetailFeign.findDetailByOrderCode(orderInfo.getCode());

        String materialTypeCode = "";
        String materialTypeName = "";

//        EntityWrapper<SaleCourseManage> scEw = new EntityWrapper<>();
//        SaleCourseManage saleCourseManage = new SaleCourseManage();
//        saleCourseManage.setCode(code);
//        saleCourseManage.setIsDeleted(0);
//        scEw.setEntity(saleCourseManage);
//        SaleCourseManage saleCourseManageDb = manageRepository.selectOne(scEw);
//        if(ObjectUtils.isEmpty(saleCourseManage)){
//            throw new BusinessException("未查到售中业务单：【" + code + "】");
//        }
//        //物料数据
//        List<SaleAfterOrderDetailDTO> details = new ArrayList<>();
//        EntityWrapper<SaleCourseMaterial> scDEw = new EntityWrapper<>();
//        SaleCourseMaterial queryEntity=new SaleCourseMaterial();
//        queryEntity.setSaleCourceId(saleCourseManage.getId());
//        scDEw.setEntity(queryEntity);
//        List<SaleCourseMaterial> mList = courseMaterialRepository.selectList(scDEw);

        //报价系统获取项目经理价
        BidPmsPriceRequestParam bidPmsPriceRequestParam = new BidPmsPriceRequestParam();
        List<BidPmsSkuCodeVo> skuCodeVos = Lists.newArrayList();
        omsOrderDetailList.forEach(item->{
            BidPmsSkuCodeVo skuCodeVo = new BidPmsSkuCodeVo();
            skuCodeVo.setSkuCode(item.getSkuCode());
            skuCodeVos.add(skuCodeVo);
        });

        bidPmsPriceRequestParam.setPriceType("projectManagerPrice");
        bidPmsPriceRequestParam.setSkuCodes(skuCodeVos);
        bidPmsPriceRequestParam.setBizopId(projectInfo.getTrackId());
        Map<String,BidPmsPriceVo> priceVoMap = bidPmsFeign.getPrice(bidPmsPriceRequestParam);

        String orderCode = codeRuleFeign.generateCode(BIZ_KEY, projectInfo.getCompanyCode().substring(0, 3));
        ProjectOrder order = new ProjectOrder();
        order.setSourceOrder(orderInfo.getCode());
        order.setProjectId(projectInfo.getId());
        order.setOrderCode(orderCode);
        order.setEmergencyFlag(true);
        order.setExpectUseDate(orderResponse.getOperateTime());
        order.setMaterialTypeCode(null);
        order.setMaterialTypeName(null);
        order.setPlanTaskId(0L);
        order.setStatus(PmsOrderStatus.PO_SUBMITTED.getCode());
        order.setOrderType(PmsOrderType.PO_NORMAL.getCode());
        order.fillOperationInfo(orderResponse.getOperator());
        orderRepository.insert(order);

        LOGGER.info("######[ProjectOrderServiceImpl.createSaleCourceOrder]开始创建订单，订单单号：[" + orderCode + "]######");

        StoreLocation location = findStoreLocation(projectInfo.getCompanyCode());

        //2.插入明细表数据
        List<ProjectOrderDetail> orderDetailList = Lists.newArrayList();
        for (OrderDetail detailResquest : omsOrderDetailList) {
            ProjectMaterial copy = new ProjectMaterial();
            SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(detailResquest.getSkuCode());
            copy.setOrderConvertNum(new BigDecimal(detailResquest.getQuantity()));
            if (skuRelationDataVo != null) {
                //采购单位 //销售单位
                List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                //获取采购单位和销售单位根据类型
                if (!ObjectUtils.isEmpty(spuUnitList)) {
                    for (SpuUnit unit : spuUnitList) {
                        if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                            BigDecimal saleNum = copy.getOrderConvertNum().divide(unit.getConvertRate(),unit.getDecimalLength(),BigDecimal.ROUND_HALF_UP).setScale(BigDecimal.ROUND_HALF_UP,unit.getDecimalLength());
                            copy.setOrderNum(saleNum);
                            copy.setSaleUnit(unit.getUnitName());
                        }
                        if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                            copy.setConvertUnit(unit.getUnitName());
                        }
                    }
                }
            }
            //特项代码
            copy.setSpecialCode(skuRelationDataVo.getSpu().getSpecialCode());

            //获取项目经理价格
            if(ObjectUtils.isEmpty(priceVoMap.get(copy.getMaterialCode()))){
                copy.setProManagerPrice(BigDecimal.ZERO);
            }else{
                if(StringUtils.isNotBlank(priceVoMap.get(copy.getMaterialCode()).getErrorMessage())){
                    copy.setProManagerPrice(BigDecimal.ZERO);
                }else {
                    copy.setProManagerPrice(priceVoMap.get(copy.getMaterialCode()).getPrice());
                }
            }
            BigDecimal cost = saleCourseManageDb.getProjectManagerAmount()==null?BigDecimal.ZERO:saleCourseManageDb.getProjectManagerAmount();
            if(cost.compareTo(new BigDecimal(1000)) <0){
                //copy.setProManagerPrice(BigDecimal.ZERO);
            }
            //类别
            Category category = categoryFeign.getById(skuRelationDataVo.getSpu().getCategoryId());
            copy.setMaterialType(category.getCode());
            copy.setMaterialTypeName(category.getName());
            Category middleCategory = categoryFeign.getById(category.getParentId());
            copy.setMaterialMiddleType(middleCategory.getCode());
            copy.setMaterialMiddleTypeName(middleCategory.getName());

            materialTypeCode = middleCategory.getCode();
            materialTypeName = middleCategory.getName();

            copy.setProjectId(projectInfo.getId());
            copy.setMaterialCode(skuRelationDataVo.getSkuCode());
            copy.setMaterialName(skuRelationDataVo.getSku().getDescription());
            copy.setId(null);
            copy.setComfirmDetailId(0L);
            copy.setAddFlag(true);
            copy.setPickingNum(BigDecimal.ZERO);
            copy.setPickingConvertNum(BigDecimal.ZERO);
            copy.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
            copy.fillOperationInfo(orderResponse.getOperator());
            //查询该项目中sku相同的设计师选材已下单的材料
            EntityWrapper<ProjectMaterial> entityWrapper = new EntityWrapper<>();
            entityWrapper.where("is_deleted = 0 and project_id = " + projectInfo.getId() + " and material_code = '"+ skuRelationDataVo.getSkuCode() +"' and order_num>0");
            List<ProjectMaterial> materialList = materialRepository.selectList(entityWrapper);
            if(UtilTool.isNotNull(materialList)){
                copy.setComfirmDetailId(materialList.get(0).getComfirmDetailId());
                copy.setPackageId(materialList.get(0).getPackageId());
            }
            materialRepository.insert(copy);

            //插入状态记录表
            ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
            materialTrace.setFollowUserCode(orderResponse.getOperator());
            materialTrace.setProjectId(projectInfo.getId());
            materialTrace.setProjectMaterialId(copy.getId());
            materialTrace.setFollowTime(new Date());
            materialTrace.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
            materialTrace.setFollowUserName(orderResponse.getOperatorName());
            materialTrace.fillOperationInfo(orderResponse.getOperator());
            traceRepository.insert(materialTrace);


            ProjectOrderDetail orderDetail = new ProjectOrderDetail();
            orderDetail.setMaterialCode(detailResquest.getSkuCode());
            orderDetail.setMaterialName(detailResquest.getSkuName());
            //采购单位数量
            orderDetail.setOrderConvertNum(new BigDecimal(detailResquest.getQuantity()));
            //销售单位数量
            if (skuRelationDataVo != null) {
                //采购单位 //销售单位
                List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                //获取采购单位和销售单位根据类型
                if (!ObjectUtils.isEmpty(spuUnitList)) {
                    for (SpuUnit unit : spuUnitList) {
                        if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                            BigDecimal saleNum = orderDetail.getOrderConvertNum().divide(unit.getConvertRate(),unit.getDecimalLength(),BigDecimal.ROUND_HALF_UP).setScale(BigDecimal.ROUND_HALF_UP,unit.getDecimalLength());
                            orderDetail.setOrderNum(saleNum);
                            orderDetail.setSaleUnit(unit.getUnitName());
                        }
                        if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                            orderDetail.setConvertUnit(unit.getUnitName());
                        }
                    }
                }
            }
            orderDetail.setRemark(detailResquest.getRemark());
            if(ObjectUtils.isEmpty(priceVoMap.get(orderDetail.getMaterialCode()))){
                orderDetail.setProManagerPrice(null);
            }else{
                BidPmsPriceVo pmsPriceVo = priceVoMap.get(orderDetail.getMaterialCode());
                orderDetail.setProManagerPrice(pmsPriceVo.getPrice());
            }

            orderDetail.setProjectMaterialId(copy.getId());
            orderDetail.setOrderId(order.getId());
            orderDetail.setProjectId(projectInfo.getId());
            orderDetail.setSpecialCode(skuRelationDataVo.getSpu().getSpecialCode());
            orderDetail.setStatus(PmsMaterialStatus.M_ORDERED.getCode());

            //绑定行号
            orderDetail.setItemId(detailResquest.getSourceLineNo().toString());

            // 设置明细是否入库属性
            Inventory inventory = inventoryFeign.findLastInventory(location.getId(),detailResquest.getSkuCode());
            if (inventory == null) {
                throw new BusinessException("库存属性不存在，编码：【" + detailResquest.getSkuCode() + "】");
            }
            if(ObjectUtils.isEmpty(inventory.getInStoreFlag())){
                LOGGER.info("######[ProjectOrderServiceImpl.createSaleCourceOrder]sku结果：[" + detailResquest.getSkuCode() + "是否入库属性为空过滤######");
                orderDetail.setInStore(true);
            }
            orderDetail.setInStore(inventory.getInStoreFlag());
            orderDetail.setInStore(detailResquest.getPreStore());
            orderDetail.setOmsOrderCode(orderInfo.getCode());
            orderDetail.fillOperationInfo(orderResponse.getOperator());
            orderDetailList.add(orderDetail);
        }
        orderDetailRepository.insertBatch(orderDetailList);


        order.setMaterialTypeCode(materialTypeCode);
        order.setMaterialTypeName(materialTypeName);
        orderRepository.updateById(order);

        //3.更新物料类别状态为已下单
        updateMaterialType(projectInfo.getId(),orderResponse.getOperator(),materialTypeCode);
    }


    private void updateMaterialType(Long projectId,String operator,String materialTypeCode) {
        ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(projectId, materialTypeCode);
        if(ObjectUtils.isEmpty(materialType)){
            materialType.setMaterialType(materialType.getMaterialType());
            materialType.setProjectId(projectId);
            materialType.setStatus(PmsMaterialTypeStatus.MT_ORDERED.getCode());
            materialType.fillOperationInfo(operator);
            materialTypeRepository.insert(materialType);
            return;
        }
        materialType.setMaterialType(materialType.getMaterialType());
        materialType.setProjectId(projectId);
        materialType.setStatus(PmsMaterialTypeStatus.MT_ORDERED.getCode());
        materialType.fillOperationInfo(operator);
        materialTypeRepository.updateById(materialType);
    }




    private void buildOrderChainInfo(@RequestBody PmsOrderStateResponse orderReturnVo, ProjectInfo info, ProjectOrder order) {
        List<PmsOrderStateDetailResponse> orderStateDetailResponseList = orderReturnVo.getDetails();
        List<PmsOrderStateDetailResponse> notInStoreList = Lists.newArrayList();
        for(PmsOrderStateDetailResponse detailResponse:orderStateDetailResponseList){
            ProjectOrderDetail orderDetail = orderDetailRepository.selectOrderDetail(detailResponse.getLineNo(),info.getId());
            if (!ObjectUtils.isEmpty(orderDetail.getInStore()) && orderDetail.getInStore()) {
                notInStoreList.add(detailResponse);
            }
        }
        if(ObjectUtils.isEmpty(notInStoreList)){
            return;
        }

        LOGGER.info("######[ProjectOrderServiceImpl.updateOrderStatus]地采订单生成待签收运单数据[" + orderReturnVo.getSourceCode() + "] 开始######");

        Map<String, List<PmsOrderStateDetailResponse>> map = notInStoreList.stream().collect(Collectors.groupingBy(PmsOrderStateDetailResponse::getBillCode));
        for (Map.Entry<String, List<PmsOrderStateDetailResponse>> entry : map.entrySet()) {
            MaterialCheckRecord checkRecord = new MaterialCheckRecord();
            checkRecord.setPickDeliveryCode(codeRuleFeign.generateCode(RuleCode.PMS_PICKING_DELIVERY, info.getCompanyCode().substring(0, 3)));
            checkRecord.setProjectId(info.getId());
            checkRecord.setRecordStatus(PmsCheckReportStatus.CP_TOBECHECKED.getCode());
            checkRecord.setVoteAgree("1");
            checkRecord.setVoteContent("同意");
            checkRecord.fillOperationInfo(orderReturnVo.getOperator());
            recordRepository.insert(checkRecord);

            for (PmsOrderStateDetailResponse detailVo : entry.getValue()) {
                ProjectOrderDetail orderDetail = orderDetailRepository.selectOrderDetail(detailVo.getLineNo(),info.getId());
                orderDetail.setStatus(PmsMaterialStatus.M_DELIVERED.getCode());
                orderDetailRepository.updateById(orderDetail);

                ProjectPickingDetail pickingDetail = pickingDetailRepository.selectOneByOrderDetailId(orderDetail.getId(),info.getId());
                pickingDetail.setStatus(PmsMaterialStatus.M_DELIVERED.getCode());
                pickingDetailRepository.updateById(pickingDetail);

                ProjectPicking picking = pickingRepository.selectById(pickingDetail.getPickingId());

                //添加需要验收的材料运单信息
                ProjectPickingMaterialChain materialChain = new ProjectPickingMaterialChain();
                materialChain.setCheckId(checkRecord.getId());
                materialChain.setProjectMaterialId(orderDetail.getProjectMaterialId());
                materialChain.setCompanyCode(info.getCompanyCode());
                materialChain.setDeliveryCode(info.getDeliveryDepartmentCode());
                materialChain.setProjectId(info.getId());
                materialChain.setMaterialCode(orderDetail.getMaterialCode());
                materialChain.setMaterialName(orderDetail.getMaterialName());
                materialChain.setPickingCode(picking.getPickingCode());
                materialChain.setStatus(PmsCheckReportStatus.CP_TOBECHECKED.getCode());
                materialChain.setPickingDetailId(pickingDetail.getId());
                materialChain.setOrderDetailId(pickingDetail.getOrderDetailId());
                materialChain.setMaterialNum(pickingDetail.getPickingConvertNum().stripTrailingZeros().toPlainString());
                materialChain.fillOperationInfo(orderReturnVo.getOperator());
                chainRepository.insert(materialChain);
            }
        }


        LOGGER.info("######[ProjectOrderServiceImpl.updateOrderStatus]地采订单生成待签收运单数据[" + orderReturnVo.getSourceCode() + "] 结束######");
    }

    @Override
    @Transactional
    public void orderReturn(@RequestBody PmsOrderResponse orderResponse) {
        String state = orderResponse.getState();
        final ProjectOrder order = orderRepository.selectOrderByCode(orderResponse.getSourceCode());
        if (state.equals(OrderResponseState.FAILURE)) {

            //取消
            cancelOrderDataReturn(order,orderResponse);
        } else if (state.equals(OrderResponseState.SUCCESS)) {

            //成功
            orderReturnSuccess(orderResponse, order);

        } else if (state.equals(OrderResponseState.PORTION_FAILURE)) {

            //部分成功 --- 订单全部取消，生成死货领料单
            orderReturnPartSuccess(orderResponse, order);
        } else {

            LOGGER.error("######[ProjectOrderServiceImpl.orderReturn]状态传输错误,订单[" + order.getOrderCode() +"]######");
            throw new BusinessException("状态传输错误!");
        }
    }

    @Override
    @Transactional
    public void pickingReturn(@RequestBody PmsOrderResponse orderResponse) {
        String state = orderResponse.getState();
        final ProjectPicking picking = pickingRepository.selectPickingByPickingCode(orderResponse.getSourceCode());
        if (state.equals(OrderResponseState.FAILURE)) {
            //取消领料单
            cancelPickingDataReturn(picking,orderResponse.getMessage());
        } else if (state.equals(OrderResponseState.SUCCESS)) {
            //成功
            pickingReturnSuccess(orderResponse, picking);
        } else if (state.equals(OrderResponseState.PORTION_FAILURE)) {

            LOGGER.error("######[ProjectOrderServiceImpl.pickingReturn]状态传输错误,领料单[" + picking.getPickingCode() +"]不存在部分失败情况######");
            throw new BusinessException("状态传输错误,领料单不存在部分失败情况!");
        } else {

            LOGGER.error("######[ProjectOrderServiceImpl.pickingReturn]状态传输错误,领料单[" + picking.getPickingCode() +"]######");
            throw new BusinessException("状态传输错误!");
        }

    }

    private void orderReturnPartSuccess(@RequestBody PmsOrderResponse orderResponse, ProjectOrder order) {
        LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，正在更新订单状态，订单单号[" + order.getOrderCode() + "]######");

        //1 订单状态已申请
        ProjectOrder updateOrder = new ProjectOrder();
        updateOrder.setId(order.getId());
        updateOrder.setStatus(PmsOrderStatus.PO_SUBMITTED.getCode());
        updateOrder.fillOperationInfo("admin");
        orderRepository.updateById(updateOrder);
        //2 物料状态为已下单
        List<PmsOrderDetailResponse> oldDetailRequestList = orderResponse.getOldLines();
        List<PmsOrderDetailResponse> newDetailRequestList = orderResponse.getNewLines();
        //List<String>oldItemIdList = detailRequestList.stream().map(PmsOrderDetailResponse::getLineNo).collect(Collectors.toList());
        List<String> newItemIdList = newDetailRequestList.stream().map(PmsOrderDetailResponse::getLineNo).collect(Collectors.toList());

        if (!ObjectUtils.isEmpty(oldDetailRequestList)) {
            //2.更新物料状态和量
            List<Long> ids = Lists.newArrayList();//主材id
            List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());
            //从订单明细中获取主材ids
            List<Long> orderIds = UtilTool.getListObjectByField(orderDetailList, "projectMaterialId");
            ids.addAll(orderIds);

            final ProjectPicking picking = pickingRepository.selectPickingByOrderCode(order.getOrderCode());
            List<ProjectPickingDetail> pickingDetailList = null;
            if (!Objects.isNull(picking)) {
                pickingDetailList = pickingRepository.selectPickingDetail(picking.getId());
                //从领料单明细中获取主材ids
                List<Long> pickingIds = UtilTool.getListObjectByField(pickingDetailList, "projectMaterialId");
                ids.addAll(pickingIds);
            }

            Map<Long, ProjectMaterial> materialMap = new HashMap<>();
            List<ProjectMaterial> list = materialRepository.selectBatchIds(ids.stream().distinct().collect(Collectors.toList()));
            if (!ObjectUtils.isEmpty(list)) {
                for (ProjectMaterial material : list) {
                    materialMap.put(material.getId(), material);
                }
            }

            for (PmsOrderDetailResponse detailResponse : oldDetailRequestList) {
                final ProjectOrderDetail orderDetail = orderDetailRepository.selectById(Long.parseLong(detailResponse.getLineNo()));
                ProjectMaterial material = materialMap.get(orderDetail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }

                ProjectOrderDetail updateDetail = new ProjectOrderDetail();
                updateDetail.setId(orderDetail.getId());
                updateDetail.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                updateDetail.setOmsOrderCode(detailResponse.getOrderCode());
                updateDetail.fillOperationInfo("admin");
                //有死货更新订单明细量，物料总量不更新
                if(newItemIdList.contains("1"+detailResponse.getLineNo())){
                    LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，出现死货明细行更新状态和量，订单行号[1" + detailResponse.getLineNo() + "]######");
                    SpuUnit unit = getSaleUnitBySkuCode(material);
                    updateDetail.setOrderNum(new BigDecimal(detailResponse.getQuantity()).divide(unit.getConvertRate(),BigDecimal.ROUND_UP, unit.getDecimalLength()));
                    updateDetail.setOrderConvertNum(new BigDecimal(detailResponse.getQuantity()));
                    //更新物料下单量（主材总量-（订单明细死货之间的差额））
                    material.setOrderConvertNum(material.getOrderConvertNum().subtract(orderDetail.getOrderConvertNum().subtract(updateDetail.getOrderConvertNum())));
                    material.setOrderNum(material.getOrderNum().subtract(orderDetail.getOrderNum().subtract(updateDetail.getOrderNum())));
                }
                orderDetailRepository.updateById(updateDetail);
            }

            //3.更新领料单
            if (!Objects.isNull(picking)) {
                LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，更新订单对应领料单状态，领料单单号[" + picking.getPickingCode() + "]######");

                ProjectPicking updatePicking = new ProjectPicking();
                updatePicking.fillOperationInfo("admin");
                updatePicking.setId(picking.getId());
                updatePicking.setStatus(PmsPickingStatus.PP_SUBMITTED.getCode());
                pickingRepository.updateById(updatePicking);
                for (ProjectPickingDetail detail : pickingDetailList) {
                    ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
                    if (material == null) {
                        continue;
                    }
                    material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
                    material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));
                }
            }

            if (!ObjectUtils.isEmpty(materialMap.values())) {
                LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，更新物料状态######");

                //根据物料类别查询是否存在关系数据
                List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
                List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());

                for (ProjectMaterial material : materialMap.values()) {
                    ProjectMaterial update = new ProjectMaterial();
                    update.setId(material.getId());
                    update.setOrderNum(material.getOrderNum());
                    update.setOrderConvertNum(material.getOrderConvertNum());
                    update.setPickingConvertNum(material.getPickingConvertNum());
                    update.setPickingNum(material.getPickingNum());
                    update.fillOperationInfo("admin");

                    if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                        //判断物料是否需要测量
                        if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                        } else {
                            update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                        }
                    }else{
                        update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                    }

                    materialRepository.updateById(update);

                    //插入状态记录表
                    ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                    materialTrace.setFollowUserCode("admin");
                    materialTrace.setProjectId(material.getProjectId());
                    materialTrace.setProjectMaterialId(material.getId());
                    materialTrace.setFollowTime(new Date());
                    materialTrace.setStatus(update.getStatus());
                    materialTrace.setFollowUserName("admin");
                    materialTrace.fillOperationInfo("admin");
                    traceRepository.insert(materialTrace);

                }
            }
        }

        //新的行号则添加新的物料--死货
        if (!ObjectUtils.isEmpty(newDetailRequestList)) {
            LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，开始处理死货,行号[" + StringUtils.join(newDetailRequestList,",") + "]######");

            //生成新的领料单死货类型的
            ProjectPicking picking = new ProjectPicking();
            picking.setStatus(PmsPickingStatus.PP_APPLYING.getCode());
            picking.setPickingType(PmsPickingType.PP_DEAD.getCode());
            //此处领料单编号使用订单系统领料编号
            picking.setPickingCode(newDetailRequestList.get(0).getOrderCode());
            picking.setProjectId(order.getProjectId());
            picking.setOrderCode(order.getOrderCode());
            picking.setEmergencyFlag(order.getEmergencyFlag());
            picking.setExpectUseDate(order.getExpectUseDate());
            picking.fillOperationInfo("admin");
            pickingRepository.insert(picking);

            LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，开始处理死货,生成新死货领料单，单号[" +picking.getPickingCode() + "]######");

            //包装明细数据
            LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，开始处理死货,生成新死货领料单明细开始，单号[" +picking.getPickingCode() + "]######");
            for (PmsOrderDetailResponse detailResponse : newDetailRequestList) {
                String lineNo = detailResponse.getLineNo().substring(1, detailResponse.getLineNo().length());
                final ProjectOrderDetail orderDetail = orderDetailRepository.selectById(Long.parseLong(lineNo));
                //添加主材信息
                final ProjectMaterial material = materialRepository.selectById(orderDetail.getProjectMaterialId());
                SpuUnit unit = getSaleUnitBySkuCode(material);
                //添加新的物料add
                ProjectMaterial insertMaterial = new ProjectMaterial();
                BeanUtils.copy(material, insertMaterial);
                insertMaterial.setId(null);
                insertMaterial.setAddFlag(true);
                insertMaterial.setStatus(PmsMaterialStatus.M_IN_STORAGE.getCode());
                insertMaterial.setOrderConvertNum(new BigDecimal(detailResponse.getQuantity()));
                insertMaterial.setOrderNum(new BigDecimal(detailResponse.getQuantity()).divide(unit.getConvertRate(),BigDecimal.ROUND_UP, unit.getDecimalLength()));
                insertMaterial.setPickingNum(insertMaterial.getOrderNum());
                insertMaterial.setPickingConvertNum(insertMaterial.getOrderConvertNum());
                insertMaterial.fillOperationInfo("admin");
                materialRepository.insert(insertMaterial);

                //添加订单明细中的主材id，行号
                ProjectOrderDetail insertDetail = new ProjectOrderDetail();
                BeanUtils.copy(orderDetail, insertDetail);
                insertDetail.setId(null);
                insertDetail.setOrderConvertNum(new BigDecimal(detailResponse.getQuantity()));
                insertDetail.setOrderNum(insertMaterial.getOrderNum());
                insertDetail.setItemId(detailResponse.getLineNo());
                insertDetail.setProjectMaterialId(insertMaterial.getId());
                insertDetail.setOmsOrderCode(detailResponse.getOrderCode());
                insertDetail.setOrderId(order.getId());
                insertDetail.fillOperationInfo("admin");
                orderDetailRepository.insert(insertDetail);

                //添加领料单明细
                ProjectPickingDetail pickingDetail = new ProjectPickingDetail();
                pickingDetail.setOrderCode(order.getOrderCode());
                pickingDetail.setOrderDetailId(insertDetail.getId());
                pickingDetail.setPickingConvertNum(insertDetail.getOrderConvertNum());
                pickingDetail.setPickingNum(insertDetail.getOrderNum());
                pickingDetail.setPickingId(picking.getId());
                pickingDetail.setProjectId(picking.getProjectId());
                pickingDetail.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                pickingDetail.setSpecialCode(material.getSpecialCode());
                pickingDetail.setProjectMaterialId(insertMaterial.getId());
                pickingDetail.setMaterialCode(material.getMaterialCode());
                pickingDetail.setMaterialName(material.getMaterialName());
                pickingDetail.setProManagerPrice(material.getProManagerPrice());
                pickingDetail.setOmsOrderCode(detailResponse.getOrderCode());
                pickingDetail.setConvertUnit(material.getConvertUnit());
                pickingDetail.setSaleUnit(material.getSaleUnit());
                pickingDetail.fillOperationInfo("admin");
                pickingDetailRepository.insert(pickingDetail);

                //包装行号
                ProjectPickingDetail updatePickingDetail = new ProjectPickingDetail();
                updatePickingDetail.setId(pickingDetail.getId());
                updatePickingDetail.setItemId(pickingDetail.getId().toString());
                pickingDetailRepository.updateById(updatePickingDetail);
            }

            LOGGER.info("######[ProjectOrderServiceImpl.orderReturnPartSuccess]订单返回状态为部分成功标识，开始处理死货,生成新死货领料单明细结束，单号[" +picking.getPickingCode() + "]######");
        }
    }


    private void orderReturnSuccess(@RequestBody PmsOrderResponse orderResponse, ProjectOrder order) {
        LOGGER.info("######[ProjectOrderServiceImpl.orderReturnSuccess]订单返回状态为成功标识，正在更新订单状态，订单单号[" + order.getOrderCode() + "]######");

        //成功
        //1 订单状态已申请
        ProjectOrder updateOrder2 = new ProjectOrder();
        updateOrder2.setId(order.getId());
        updateOrder2.setStatus(PmsOrderStatus.PO_SUBMITTED.getCode());
        updateOrder2.fillOperationInfo("admin");
        orderRepository.updateById(updateOrder2);
        //2 物料状态为已下单
        List<PmsOrderDetailResponse> detailRequestList = orderResponse.getOldLines();
        if (!ObjectUtils.isEmpty(detailRequestList)) {
            List<Long> ids = Lists.newArrayList();
            for (PmsOrderDetailResponse detailResponse : detailRequestList) {
                ProjectOrderDetail orderDetail = orderDetailRepository.selectOrderDetail(detailResponse.getLineNo(),order.getProjectId());
                if(UtilTool.isNotNull(orderDetail)){
                    ids.add(orderDetail.getProjectMaterialId());
                }
            }

            Map<Long, ProjectMaterial> materialMap = new HashMap<>();
            List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
            Set<String> typeSet = Sets.newHashSet();
            if (!ObjectUtils.isEmpty(list)) {
                for (ProjectMaterial material : list) {
                    materialMap.put(material.getId(), material);
                    typeSet.add(material.getMaterialMiddleType());
                }
            }

            ProjectPicking picking = pickingRepository.selectPickingByOrderCode(order.getOrderCode());
            if(!ObjectUtils.isEmpty(picking)){
                picking.setStatus(PmsPickingStatus.PP_SUBMITTED.getCode());
                picking.fillOperationInfo("admin");
                pickingRepository.updateById(picking);
            }

            for (PmsOrderDetailResponse detailResponse : detailRequestList) {
                final ProjectOrderDetail orderDetail = orderDetailRepository.selectOrderDetail(detailResponse.getLineNo(),order.getProjectId());
                //添加主材信息
                ProjectMaterial material = materialMap.get(orderDetail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                //SpuUnit unit = getSaleUnitBySkuCode(material);
                //material.setOrderNum(material.getOrderNum().subtract((orderDetail.getOrderConvertNum().subtract(new BigDecimal(detailResponse.getQuantity())).divide(unit.getConvertRate()).setScale(BigDecimal.ROUND_UP, unit.getDecimalLength()))));
                //material.setOrderConvertNum(material.getOrderConvertNum().subtract(orderDetail.getOrderConvertNum()));
                material.fillOperationInfo("admin");


                ProjectOrderDetail updateDetail = new ProjectOrderDetail();
                updateDetail.setId(orderDetail.getId());
                updateDetail.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                //updateDetail.setOrderNum(new BigDecimal(detailResponse.getQuantity()).divide(unit.getConvertRate(),unit.getDecimalLength(),BigDecimal.ROUND_UP));
                //updateDetail.setOrderConvertNum(new BigDecimal(detailResponse.getQuantity()));
                updateDetail.setOmsOrderCode(detailResponse.getOrderCode());
                updateDetail.setInStore(detailResponse.getDirectDelivery());
                //updateDetail.setInStore(detailResponse.getInStore());
                updateDetail.fillOperationInfo("admin");
                orderDetailRepository.updateById(updateDetail);


                if (!Objects.isNull(picking)) {
                    final ProjectPickingDetail pickingDetail = pickingDetailRepository.selectOneByOrderDetailId(orderDetail.getId(),picking.getProjectId());
                    //material.setPickingNum(material.getOrderNum().subtract(orderDetail.getOrderConvertNum().subtract(new BigDecimal(detailResponse.getQuantity())).divide(unit.getConvertRate(),unit.getDecimalLength(),BigDecimal.ROUND_UP)));
                    //material.setPickingConvertNum(material.getPickingConvertNum().subtract(pickingDetail.getPickingConvertNum().subtract(new BigDecimal(detailResponse.getQuantity()))));
                    //material.fillOperationInfo("admin");

                    ProjectPickingDetail updatePicking = new ProjectPickingDetail();
                    updatePicking.setId(pickingDetail.getId());
                    updatePicking.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                    //updatePicking.setPickingNum(new BigDecimal(detailResponse.getQuantity()).divide(unit.getConvertRate()).setScale(BigDecimal.ROUND_UP, unit.getDecimalLength()));
                    //updatePicking.setPickingConvertNum(new BigDecimal(detailResponse.getQuantity()));
                    updatePicking.setOmsOrderCode(detailResponse.getOrderCode());
                    updatePicking.fillOperationInfo("admin");
                    pickingDetailRepository.updateById(updatePicking);
                }
            }

            LOGGER.info("######[ProjectOrderServiceImpl.orderReturnSuccess]订单返回状态为成功标识，正在更新订单物料状态，订单单号[" + order.getOrderCode() + "]######");
            if (!ObjectUtils.isEmpty(materialMap.values())) {
                //根据物料类别查询是否存在关系数据
                List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
                List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());
                for (ProjectMaterial material : materialMap.values()) {
                    ProjectMaterial update = new ProjectMaterial();
                    update.setId(material.getId());
                    update.setOrderNum(material.getOrderNum());
                    update.setOrderConvertNum(material.getOrderConvertNum());
                    update.setPickingConvertNum(material.getPickingConvertNum()==null?BigDecimal.ZERO:material.getPickingConvertNum());
                    update.setPickingNum(material.getPickingNum()==null?BigDecimal.ZERO:material.getPickingNum());
                    update.fillOperationInfo("admin");

                    if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                        //判断物料是否需要测量
                        if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                        } else {
                            update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                        }
                    }else{
                        update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                    }
                    materialRepository.updateById(update);

                    //插入状态记录表
                    ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                    materialTrace.setFollowUserCode("admin");
                    materialTrace.setProjectId(material.getProjectId());
                    materialTrace.setProjectMaterialId(material.getId());
                    materialTrace.setFollowTime(new Date());
                    materialTrace.setStatus(update.getStatus());
                    materialTrace.setFollowUserName("admin");
                    materialTrace.fillOperationInfo("admin");
                    traceRepository.insert(materialTrace);
                }
            }

            LOGGER.info("######[ProjectOrderServiceImpl.orderReturnSuccess]订单返回状态为成功标识，正在更新订单物料类别状态，订单单号[" + order.getOrderCode() + "]######");
            //5.更新物料类别状态
            for (String type : typeSet) {
                ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(order.getProjectId(), type);
                if (PmsOrderType.PO_AUTO.getCode().equals(order.getOrderType()) || ObjectUtils.isEmpty(materialType)) {
                    continue;
                }
                ProjectMaterialType updateType = new ProjectMaterialType();
                updateType.setId(materialType.getId());
                updateType.setStatus(PmsMaterialTypeStatus.MT_ORDERED.getCode());
                materialTypeRepository.updateById(updateType);
            }

            LOGGER.info("######[ProjectOrderServiceImpl.orderReturnSuccess]订单返回状态为成功标识，更新完成######");
        }
    }


    private void pickingReturnSuccess(@RequestBody PmsOrderResponse orderResponse, ProjectPicking order) {
        LOGGER.info("######[ProjectOrderServiceImpl.pickingReturnSuccess]领料单返回状态为成功标识，正在更新订单状态，单号[" + order.getPickingCode() + "]######");

        //成功
        //1 订单状态已申请
        ProjectPicking updateOrder2 = new ProjectPicking();
        updateOrder2.setId(order.getId());
        updateOrder2.setStatus(PmsPickingStatus.PP_SUBMITTED.getCode());
        updateOrder2.fillOperationInfo("admin");
        pickingRepository.updateById(updateOrder2);

        LOGGER.info("######[ProjectOrderServiceImpl.pickingReturnSuccess]领料单返回状态为成功标识，正在更新订单行状态，单号[" + order.getPickingCode() + "]######");
        //2 物料状态为已下单
        List<PmsOrderDetailResponse> detailRequestList = orderResponse.getOldLines();
        if (!ObjectUtils.isEmpty(detailRequestList)) {
            List<Long> ids = Lists.newArrayList();
            for (PmsOrderDetailResponse detailResponse : detailRequestList) {
                final ProjectPickingDetail pickingDetail = pickingDetailRepository.selectOneByItemId(detailResponse.getLineNo(),order.getProjectId());
                if(!ObjectUtils.isEmpty(pickingDetail)){
                    ids.add(pickingDetail.getProjectMaterialId());
                }
                ProjectPickingDetail updatePicking = new ProjectPickingDetail();
                updatePicking.setId(pickingDetail.getId());
                updatePicking.setStatus(PmsMaterialStatus.M_PICKED.getCode());
                updatePicking.setPickingConvertNum(new BigDecimal(detailResponse.getQuantity()));
                updatePicking.setOmsOrderCode(detailResponse.getOrderCode());
                updatePicking.fillOperationInfo("admin");
                pickingDetailRepository.updateById(updatePicking);
            }

            List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
            Set<String> typeSet = Sets.newHashSet();
            if(CollectionUtils.isNotEmpty(list)){
                for (ProjectMaterial material : list) {
                    typeSet.add(material.getMaterialMiddleType());
                    ProjectMaterial updateMaterial = new ProjectMaterial();
                    updateMaterial.setId(material.getId());
                    updateMaterial.setStatus(PmsMaterialStatus.M_PICKED.getCode());
                    updateMaterial.fillOperationInfo("admin");
                    materialRepository.updateById(updateMaterial);
                    //插入状态记录表
                    ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                    materialTrace.setFollowUserCode("admin");
                    materialTrace.setProjectId(material.getProjectId());
                    materialTrace.setProjectMaterialId(material.getId());
                    materialTrace.setFollowTime(new Date());
                    materialTrace.setStatus(material.getStatus());
                    materialTrace.setFollowUserName("admin");
                    materialTrace.fillOperationInfo("admin");
                    traceRepository.insert(materialTrace);
                }
            }

            LOGGER.info("######[ProjectOrderServiceImpl.pickingReturnSuccess]领料单返回状态为成功标识，正在更新物料类别状态，单号[" + order.getPickingCode() + "]######");
            //5.更新物料类别状态
            for (String type : typeSet) {
                //查询这些物料之外的需要领的物料的状态是否都是已领用 是：PICKED 否:不更新
                ProjectMaterial param = new ProjectMaterial();
                param.setStatus(PmsMaterialStatus.M_IN_STORAGE.getCode());
                param.setProjectId(order.getProjectId());
                EntityWrapper<ProjectMaterial> ew = new EntityWrapper<>(param);
                ew.where(" ID NOT IN (" + StringUtils.join(ids, ",") + ")");
                List<ProjectMaterial> materialList = materialRepository.selectList(ew);
                ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(order.getProjectId(), type);
                if (!ObjectUtils.isEmpty(materialList) && !ObjectUtils.isEmpty(materialType)) {
                    ProjectMaterialType updateType = new ProjectMaterialType();
                    updateType.setId(materialType.getId());
                    updateType.setStatus(PmsMaterialTypeStatus.MT_PICKED.getCode());
                    materialTypeRepository.updateById(updateType);
                }
            }
        }
    }


    @Override
    @Transactional
    public void cancelOrder(@PathVariable("orderId") Long orderId,@RequestBody ProjectOrderCancelRequest cancelRequest) {
        ProjectOrder order = orderRepository.selectById(orderId);
        order.setCancelReason(cancelRequest.getCancelReason());
        cancelPmsOrder(order,cancelRequest);
    }

    @Override
    @Transactional
    public void cancelOrderByOMS(@PathVariable("orderId") Long orderId) {
        final ProjectOrder order = orderRepository.selectById(orderId);
        cancelOrderDataByOMS(order);
    }


    @Override
    @Transactional
    public void cancelOrderByCode(@PathVariable("orderCode") String orderCode) {
        final ProjectOrder order = orderRepository.selectOrderByCode(orderCode);
        cancelOrderData(order);
    }


    @Override
    @Transactional
    public void cancelOrderPartial(PmsOrderStateResponse stateResponse, List<ProjectOrderDetail> cancelDetailList) {
        List<Long> ids = Lists.newArrayList();
        List<Long> orderIds = cancelDetailList.stream().map(ProjectOrderDetail::getProjectMaterialId).collect(Collectors.toList());
        ids.addAll(orderIds);
        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (ProjectOrderDetail detail : cancelDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setOrderNum(material.getOrderNum().subtract(detail.getOrderNum()));
            material.setOrderConvertNum(material.getOrderConvertNum().subtract(detail.getOrderConvertNum()));

            ProjectOrderDetail updateDetail = new ProjectOrderDetail();
            //updateDetail.setIsDeleted(1);
            updateDetail.setCancelReason("商家取消:" + stateResponse.getRemark());
            updateDetail.setStatus(PmsMaterialStatus.M_CANCELED.getCode());
            updateDetail.setId(detail.getId());
            updateDetail.fillOperationInfo(stateResponse.getOperator());
            orderDetailRepository.updateById(updateDetail);
        }
        //3.更新采购单明细/领料单明细 --物料实际下单量、领用量
        List<Long> orderDetailIds = cancelDetailList.stream().map(ProjectOrderDetail::getId).collect(Collectors.toList());
        if(ObjectUtils.isEmpty(orderDetailIds)){
            return;
        }
        EntityWrapper<ProjectPickingDetail> ew_p = new EntityWrapper<>();
        ew_p.in("order_detail_id", orderDetailIds);
        final List<ProjectPickingDetail> cancelPickingDetailList = pickingDetailRepository.selectList(ew_p);
        if (!ObjectUtils.isEmpty(cancelPickingDetailList)) {
            Long checkId = 0L;
            List<Long> chainIds = Lists.newArrayList();
            for (ProjectPickingDetail detail : cancelPickingDetailList) {
                ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
                material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));
                ProjectPickingDetail updateDetail = new ProjectPickingDetail();
                updateDetail.setId(detail.getId());
                updateDetail.setCancelReason("商家取消");
                updateDetail.setStatus(PmsMaterialStatus.M_CANCELED.getCode());
                updateDetail.fillOperationInfo(stateResponse.getOperator());
                pickingDetailRepository.updateById(updateDetail);

                //查询对应的验收记录
                EntityWrapper<ProjectPickingMaterialChain> entityWrapper = new EntityWrapper<>();
                entityWrapper.where("project_id={0}",material.getProjectId());
                entityWrapper.where("picking_detail_id = {0}",detail.getId());
                ProjectPickingMaterialChain chain = chainRepository.selectOne(entityWrapper);
                if(!ObjectUtils.isEmpty(chain)){
                    ProjectPickingMaterialChain updatechain = new ProjectPickingMaterialChain();
                    updatechain.setId(chain.getId());
                    updatechain.setStatus(PmsCheckReportStatus.CP_CANCELLED.getCode());
                    updatechain.setIsDeleted(IsDeletedStatus.offline.getCode());
                    updatechain.fillOperationInfo(stateResponse.getOperator());
                    chainRepository.updateById(updatechain);

                    checkId = chain.getCheckId();
                    chainIds.add(chain.getId());

                }
            }

            if(!ObjectUtils.isEmpty(chainIds)){
                //查询是否存在未签收的
                EntityWrapper<ProjectPickingMaterialChain> ew_c = new EntityWrapper<>();
                ew_c.where("check_id=" + checkId + " and id not in(" + StringUtils.join(chainIds,",") + ")");
                List<ProjectPickingMaterialChain> chainList = chainRepository.selectList(ew_c);
                if(ObjectUtils.isEmpty(chainList)){
                    //更新验收记录状态
                    MaterialCheckRecord updateRecord = new MaterialCheckRecord();
                    updateRecord.setRecordStatus(PmsCheckReportStatus.CP_CANCELLED.getCode());
                    updateRecord.setId(checkId);
                    updateRecord.setIsDeleted(IsDeletedStatus.offline.getCode());
                    checkRecordRepository.updateById(updateRecord);

                    LOGGER.info("######[ProjectOrderServiceImpl.cancelOrderPartial]更新签收运单状态，运单ID[" + checkId + "] ######");

                }
            }
        }
        //判断更新为什么状态 -- 被取消的物料是不是已测量的，是不是地材的
        updateMaterialStatus(stateResponse, materialMap,cancelPickingDetailList);
    }

    @Override
    public void cancelOrderAllByOMS(Long orderId, PmsOrderStateResponse stateResponse) {
        final ProjectOrder order = orderRepository.selectById(orderId);
        cancelOrderDataAllByOMS(order,stateResponse);
    }

    @Override
    public void validateProjectMaterialAllPicked(@RequestParam("id") Long projectId) {

        List<ProjectOrder> projectOrderDetailList = orderRepository.selectNotPickedOrderDetailList(projectId);
        if(ObjectUtils.isEmpty(projectOrderDetailList)){
            return;
        }

        List<String> list = projectOrderDetailList.stream().map(ProjectOrder::getOrderCode).collect(Collectors.toList());
        throw new BusinessException("单号：【" + StringUtils.join(list,",") +"】未全部领用");
    }

    @Override
    public void validateProjectMaterialAllReceived(@RequestParam("id") Long projectId) {
        ProjectInfo info = projectInfoRepository.selectById(projectId);

        //校验所有的地材领料单都有签收单
        List<ProjectPicking> pickingList = pickingRepository.selectNotReceivePickingList(projectId);
        if(!ObjectUtils.isEmpty(pickingList)){
            List<String> list = pickingList.stream().map(ProjectPicking::getPickingCode).collect(Collectors.toList());
            throw new BusinessException("运单：【" + StringUtils.join(list,",") + "未验收完毕,请检查关联的订单是否发确认或者出库!");
        }
        MaterialCheckRecord checkRecord = new MaterialCheckRecord();
        checkRecord.setProjectId(projectId);
        checkRecord.setIsDeleted(0);
        EntityWrapper<MaterialCheckRecord> entityWrapper = new EntityWrapper<>();
        entityWrapper.setEntity(checkRecord);
        entityWrapper.where("record_status = 'toBeChecked'");
        List<MaterialCheckRecord> checkRecordList = checkRecordRepository.selectList(entityWrapper);
        if(!ObjectUtils.isEmpty(checkRecordList)){
            List<String> list = checkRecordList.stream().map(MaterialCheckRecord::getPickDeliveryCode).collect(Collectors.toList());
            throw new BusinessException("运单：【" + StringUtils.join(list,",") + "未验收完毕,请项目经理在APP签收管理模块完成签收!");
        }

        Integer integer = orderInfoFeign.findUnSendEbsCountByProject(info.getProjectCode());
        if(integer > 0){
            throw new BusinessException("还存在未签收数量:[" + integer + "]，项目ID:[" + projectId + "]");
        }
    }

    @Override
    public List<ProjectControlCostVo> findProjectMaterialControl(@PathVariable("id") Long projectId) {
        List<ProjectControlCostVo> controlCostVoList = Lists.newArrayList();
        ProjectInfo projectInfo = projectInfoRepository.selectById(projectId);

        if(ObjectUtils.isEmpty(projectInfo)){
            throw new BusinessException("参数错误：【" + projectId + "】");
        }

        //获取成本控制信息
        CostControlCalcRequest calcRequest = new CostControlCalcRequest();
        calcRequest.setBizOpId(projectInfo.getTrackId());
        calcRequest.setCode("");
        List<ControlConfigVoResponse> controlList = costControlConfigFeign.calc(calcRequest);
        //便利成本控制将当前所有物料进行归类

        //实际须支付金额
        BigDecimal readyPayAmount = BigDecimal.ZERO;

        //计算依据支付的金额
        List<String> charNos=Lists.newArrayList();
        //获取已经交付的金额
        List<SpecialAmountVo> amounts = expensePaymentRepository.statisticsAlreadyAmount(projectId,"PICKING",charNos);

        //获取已经交付的金额
        List<SpecialAmountVo> amountsOrder = expensePaymentRepository.statisticsAlreadyAmount(projectId,"ORDER",charNos);

        if(ObjectUtils.isEmpty(controlList)){
            return controlCostVoList;
        }

        for (ControlConfigVoResponse response : controlList) {
            //已交付金额
            BigDecimal alreadyAmount = getAlreadyAmount(response.getCode(), amounts);

            //已交付金额
            BigDecimal alreadyAmount2 = getAlreadyAmount(response.getCode(), amountsOrder);

            ProjectControlCostVo controlCostVo = new ProjectControlCostVo();
            BeanUtils.copy(response,controlCostVo);
            controlCostVo.setPayAmount(alreadyAmount.add(alreadyAmount2));
            controlCostVoList.add(controlCostVo);
        }
        return controlCostVoList;
    }

    @Override
    public List<ProjectMaterialType> findProjectAutoMaterialType(@PathVariable("id") Long projectId) {


        return materialTypeRepository.findProjectAutoMaterialType(projectId);
    }

    @Override
    public PmProjectOrderCheckExcessDto validateSaleOrderExcess(@RequestParam("id") Long saleId) {
        //查询相关售后所需材料


        return null;
    }

    @Override
    public List<ProjectOrderDetail> selectListByProjectMaterialId(@RequestParam("id") Long projectMaterialId) {
        return orderDetailRepository.selectListByProjectMaterialId(projectMaterialId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void addSoftProjectPickingInfo(@RequestParam("trackId") Long trackId, @RequestParam("auditor")String auditor, @RequestParam("auditorName")String auditorName){
        ProjectInfo projectInfo = projectInfoRepository.selectByTrackId(trackId);
        if(ObjectUtils.isEmpty(projectInfo)){
            throw new BusinessException("项目不存在,商机ID：[" + trackId + "]" );
        }

        //过滤之前已经下过单--查询初始状态的物料
        EntityWrapper<ProjectMaterial> entityWrapper = new EntityWrapper<>();
        ProjectMaterial param = new ProjectMaterial();
        param.setProjectId(projectInfo.getId());
        param.setIsDeleted(0);
        param.setStatus(PmsMaterialStatus.M_IN_STORAGE.getCode());
        entityWrapper.setEntity(param);
        List<ProjectMaterial> materialList = materialRepository.selectList(entityWrapper);
        if(ObjectUtils.isEmpty(materialList)){
            //throw new BusinessException("数据查询失败：[" + projectInfo.getContractCode()+"]");
            param.setStatus(PmsMaterialStatus.M_PICKED.getCode());
            List<ProjectMaterial> pickedList = materialRepository.selectList(entityWrapper);
            if(ObjectUtils.isEmpty(pickedList)){
                throw new BusinessException("数据查询失败：[" + projectInfo.getContractCode()+"]");
            }else{
                //return;
            }
        }

        List<ProjectPickingDetailAddParam> orderDetailResquestList = Lists.newArrayList();
        Map<String, PurchasePrice> skuPriceMap = new HashMap<>();
        Set<String> skuCodeSet = Sets.newHashSet();
        for (ProjectMaterial item : materialList) {
            skuCodeSet.add(item.getMaterialCode());
        }
        List<String> skuCodes = new ArrayList<>(skuCodeSet);
        StoreLocation location = findStoreLocation(projectInfo.getCompanyCode());
        skuPriceMap = buildSkuPriceMap(skuCodes, location.getId());

        // TODO: 2018/11/17  //校验下库存情况

        for(ProjectMaterial material : materialList){
            //如果该物料属性是备货的则不显示
            Inventory inventory = inventoryFeign.findLastInventory(location.getId(),material.getMaterialCode());
            if (inventory == null) {
                LOGGER.info("######[ProjectMaterialServiceImpl.buildProjectMaterialResponse]sku库存属性为空需要过滤：[" + material.getMaterialCode() + "######");
                //continue;
                throw new BusinessException("来源行%s物料库存属性未找到", material.getMaterialCode());
            }

            ProjectPickingDetailAddParam orderDetail = new ProjectPickingDetailAddParam();
            orderDetail.setMaterialCode(material.getMaterialCode());
            orderDetail.setMaterialName(material.getMaterialName());
            BidPmsBudgetDtlRequestParam budgetDtlRequestParam = new BidPmsBudgetDtlRequestParam();
            budgetDtlRequestParam.setBudgetDtlId(material.getComfirmDetailId());
            budgetDtlRequestParam.setBudgetPackDtlId(material.getPackageId());
            BidPmsBudgetDtlVo dtlVo = bidPmsFeign.getBudgetDetail(budgetDtlRequestParam);
            BigDecimal planNum = BigDecimal.ZERO;
            if(ObjectUtils.isEmpty(dtlVo)){
                //throw new BusinessException("");
            }else {
                //项目经理价
                orderDetail.setProManagerPrice(dtlVo.getPmPrice().stripTrailingZeros().toPlainString());
                //计划量
                planNum = dtlVo.getPlanNumber() == null ? BigDecimal.ZERO : dtlVo.getPlanNumber();
            }
            orderDetail.setInStoreFlag(inventory.getInStoreFlag()==true?1:0);
            orderDetail.setPreStockFlag(inventory.getPreStoreFlag()==true?1:0);

            List<ProjectOrderDetail> orderDetailList = orderRepository.selectListByProjectMaterialId(material.getId());
            if(!ObjectUtils.isEmpty(orderDetailList)){
                //如果已经下单的则默认是下单量，不允许修改
                ProjectOrderDetail projectOrderDetail = orderDetailList.get(0);
                if(!ObjectUtils.isEmpty(projectOrderDetail.getInStore()) && projectOrderDetail.getInStore()){
                    continue;
                }
                if (material.getOrderNum() != null && material.getOrderNum().compareTo(BigDecimal.ZERO) > 0) {
                    orderDetail.setPickingNum(material.getOrderNum().stripTrailingZeros().toPlainString());
                    orderDetail.setPickingConvertNum(material.getOrderConvertNum().stripTrailingZeros().toPlainString());
                    orderDetail.setInStoreFlag(1);
                    orderDetail.setPreStockFlag(0);
                }
            }else {
                //采购单位数量
                orderDetail.setPickingNum(planNum.stripTrailingZeros().toEngineeringString());
                //销售单位数量
                SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(material.getMaterialCode());
                if (skuRelationDataVo != null) {
                    //采购单位 //销售单位
                    List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                    Integer convertDecimalLenth = 0;
                    BigDecimal convertRate = BigDecimal.ONE;
                    //获取采购单位和销售单位根据类型
                    if (!ObjectUtils.isEmpty(spuUnitList)) {
                        for (SpuUnit unit : spuUnitList) {
                            if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                                //BigDecimal convertNum = planNum.multiply(unit.getConvertRate()).setScale(unit.getDecimalLength(), BigDecimal.ROUND_HALF_UP);
                                //orderDetail.setPickingConvertNum(convertNum.stripTrailingZeros().toPlainString());
                                convertRate = unit.getConvertRate();
                            }
                            if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                                convertDecimalLenth = unit.getDecimalLength();
                            }
                        }
                    }
                    orderDetail.setPickingConvertNum((planNum.multiply(convertRate).setScale(convertDecimalLenth,BigDecimal.ROUND_HALF_UP)).stripTrailingZeros().toPlainString());
                }
            }
//            orderDetail.setInStoreFlag(inventory.getInStoreFlag()==true?1:0);
//            orderDetail.setPreStockFlag(inventory.getPreStoreFlag()==true?1:0);
            orderDetail.setProManagerPrice(material.getProManagerPrice().stripTrailingZeros().toEngineeringString());
            orderDetail.setProjectMaterialId(material.getId());
            orderDetail.setRemark("");
            orderDetailResquestList.add(orderDetail);
        }
        if(ObjectUtils.isEmpty(orderDetailResquestList)){
            throw new BusinessException("领料单明细为空，不能提交，合同号：[" + projectInfo.getContractCode() + "]");
        }

        //包装领料明细
        ProjectPickingAddParam request = new ProjectPickingAddParam();
        request.setEmployeeCode(auditor);
        request.setProjectId(projectInfo.getId());
        request.setEmployeeName(auditorName);
        request.setExpectArrivalDate(DateUtil.date2Str(new Date(),"yyyy-MM-dd"));
        request.setDetails(orderDetailResquestList);
        //校验库存
        //validateStock(request);
        pickingService.addProjectPicking(request);
    }

    @Override
    public void addSoftProjectInstall(@RequestParam("trackId") Long trackId, @RequestParam("auditor")String auditor
    , @RequestParam("auditorName")String auditorName) {
        ProjectInfo projectInfo = projectInfoRepository.selectByTrackId(trackId);
        List<ProjectMaterial> materialList = materialRepository.selectAllUnInstallMaterials(projectInfo.getId());
        if(!ObjectUtils.isEmpty(materialList)){

            ProjectBatchStartInstallRequest installRequest = new ProjectBatchStartInstallRequest();
            installRequest.setProjectId(projectInfo.getId());
            installRequest.setMaterialList(materialList);
            UnifiedProfile unifiedProfile = new UnifiedProfile();
            unifiedProfile.setWorkNumber(auditor);
            unifiedProfile.setName(auditorName);
            installRequest.setUnifiedProfile(unifiedProfile);
            installService.batchStartSoftInstall(installRequest);
        }else{
            //throw new BusinessException("没有需要安装的材料，请核实!");
        }
    }

    private void updateMaterialStatus(PmsOrderStateResponse stateResponse, Map<Long, ProjectMaterial> materialMap,List<ProjectPickingDetail> cancelPickingDetailList) {
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            Set<String> typeSet = Sets.newHashSet();
            //根据物料类别查询是否存在关系数据
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());

            Long projectId = null;
            Boolean flag = false;
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(stateResponse.getOperator());
                if(ObjectUtils.isEmpty(cancelPickingDetailList)){
                    //判断物料是否需要测量
                    if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                        if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                            flag=true;
                        }
                    } else {
                        update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                    }
                }else{
                    update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                }
                //update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                materialRepository.updateById(update);


                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode(stateResponse.getOperator());
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName(stateResponse.getOperatorName());
                materialTrace.fillOperationInfo(stateResponse.getOperator());
                traceRepository.insert(materialTrace);

                typeSet.add(material.getMaterialMiddleType());
                projectId = material.getProjectId();
            }

            for (String type : typeSet) {
                final ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(projectId, type);
                if(ObjectUtils.isEmpty(materialType)){
                    continue;
                }
                //查询该类别下的物料的状态
                //查询该类别下是否还存在没测量的材料
                List<ProjectMaterial> pmList = materialRepository.selectNotMeasureList(type,projectId);
                if(ObjectUtils.isEmpty(pmList) && flag){
                    ProjectMaterialType updateType = new ProjectMaterialType();
                    updateType.setId(materialType.getId());
                    updateType.setStatus(PmsMaterialTypeStatus.MT_MEASUREED.getCode());
                    materialTypeRepository.updateById(updateType);
                }else{
                    ProjectMaterialType updateType = new ProjectMaterialType();
                    updateType.setId(materialType.getId());
                    updateType.setStatus(PmsMaterialTypeStatus.MT_ORIGINAL_STATUS.getCode());
                    materialTypeRepository.updateById(updateType);
                }

            }
        }
    }



    private void updateMaterialStatusByPmsOrderCancel(ProjectOrderCancelRequest stateResponse, Map<Long, ProjectMaterial> materialMap,List<ProjectPickingDetail> cancelPickingDetailList) {
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            Set<String> typeSet = Sets.newHashSet();
            //根据物料类别查询是否存在关系数据
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());

            Long projectId = null;
            Boolean flag = false;
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(stateResponse.getEmployeeCode());
                if(ObjectUtils.isEmpty(cancelPickingDetailList)){
                    //判断物料是否需要测量
                    if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                        if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                            flag=true;
                        }
                    } else {
                        update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                    }
                }else{
                    update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                }
                //update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                materialRepository.updateById(update);


                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode(stateResponse.getEmployeeCode());
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName(stateResponse.getEmployeeName());
                materialTrace.fillOperationInfo(stateResponse.getEmployeeCode());
                traceRepository.insert(materialTrace);

                typeSet.add(material.getMaterialMiddleType());
                projectId = material.getProjectId();
            }

            for (String type : typeSet) {
                final ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(projectId, type);
                if(ObjectUtils.isEmpty(materialType)){
                    continue;
                }
                //查询该类别下的物料的状态
                //查询该类别下是否还存在没测量的材料
                List<ProjectMaterial> pmList = materialRepository.selectNotMeasureList(type,projectId);
                if(ObjectUtils.isEmpty(pmList) && flag){
                    ProjectMaterialType updateType = new ProjectMaterialType();
                    updateType.setId(materialType.getId());
                    updateType.setStatus(PmsMaterialTypeStatus.MT_MEASUREED.getCode());
                    materialTypeRepository.updateById(updateType);
                }else{
                    ProjectMaterialType updateType = new ProjectMaterialType();
                    updateType.setId(materialType.getId());
                    updateType.setStatus(PmsMaterialTypeStatus.MT_ORIGINAL_STATUS.getCode());
                    materialTypeRepository.updateById(updateType);
                }

            }
        }
    }

    private void cancelPmsOrderDataAll(ProjectOrder order, ProjectOrderCancelRequest cancelRequest) {
        if (order.getStatus().equals(PmsOrderStatus.PO_APPLYING.getCode())) {
            throw new BusinessException("订单正在处理中，请稍后操作!");
        }
        if (order.getStatus().equals(PmsOrderStatus.PO_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }
        final ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());

        //1.更新取消状态
        ProjectOrder updateOrder = new ProjectOrder();
        updateOrder.setId(order.getId());
        updateOrder.setCancelReason("项");
        updateOrder.setStatus(PmsOrderStatus.PO_CANCELED.getCode());
        updateOrder.fillOperationInfo(cancelRequest.getEmployeeCode());
        orderRepository.updateById(updateOrder);

        //2.更新物料状态和量
        List<Long> ids = Lists.newArrayList();
        List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());

        for(ProjectOrderDetail orderDetail : orderDetailList){
            orderDetail.setCancelReason(order.getCancelReason());
            orderDetail.fillOperationInfo(cancelRequest.getEmployeeCode());
            orderDetailRepository.updateById(orderDetail);
        }

        List<Long> orderIds = UtilTool.getListObjectByField(orderDetailList, "projectMaterialId");
        final ProjectPicking picking = pickingRepository.selectPickingByOrderCode(order.getOrderCode());
        List<ProjectPickingDetail> pickingDetailList = null;
        if (!Objects.isNull(picking)) {
            pickingDetailList = pickingRepository.selectPickingDetail(picking.getId());
            List<Long> pickingIds = UtilTool.getListObjectByField(pickingDetailList, "projectMaterialId");
            ids.addAll(pickingIds);
        }
        ids.addAll(orderIds);

        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (ProjectOrderDetail detail : orderDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setOrderNum(material.getOrderNum().subtract(detail.getOrderNum()));
            material.setOrderConvertNum(material.getOrderConvertNum().subtract(detail.getOrderConvertNum()));
        }
        //3.更新领料单
        if (!Objects.isNull(picking)) {
            ProjectPicking updatePicking = new ProjectPicking();
            updatePicking.fillOperationInfo(cancelRequest.getEmployeeCode());
            updatePicking.setId(picking.getId());
            updatePicking.setStatus(PmsPickingStatus.PP_CANCELED.getCode());
            pickingRepository.updateById(updatePicking);
            for (ProjectPickingDetail detail : pickingDetailList) {
                ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
                material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));
            }
        }
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            //根据物料类别查询是否存在关系数据
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());

            Boolean flag =false;
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(cancelRequest.getEmployeeCode());

                if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                    //判断物料是否需要测量
                    if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                        if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                            flag=true;
                        }
                    } else {
                        update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                    }
                }else{
                    update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                }
               /* if (material.getPickingNum().compareTo(BigDecimal.ZERO) <= 0) {
                    update.setStatus(PmsMaterialStatus.M_IN_STORAGE.getCode());
                }*/
                materialRepository.updateById(update);


                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode(cancelRequest.getEmployeeCode());
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName(cancelRequest.getEmployeeName());
                materialTrace.fillOperationInfo(cancelRequest.getEmployeeCode());
                traceRepository.insert(materialTrace);
            }

            //查询该类别下的物料
            // updateMaterialTypeStatusToMeasured(order, materialMap);

            updateMaterialTypeStatusToMeasuredByCondition(order, materialMap, flag);
        }
    }


    public void cancelPmsOrderDataAllPartial(List<PmsCancelOrderResponse> cancelOrderResponseList,List<ProjectOrderDetail> cancelDetailList,ProjectOrderCancelRequest cancelRequest) {
        List<Long> ids = Lists.newArrayList();
        //List<Long> orderIds = UtilTool.getListObjectByField(cancelDetailList, "projectMaterialId");
        List<Long> orderIds = cancelDetailList.stream().map(ProjectOrderDetail::getProjectMaterialId).collect(Collectors.toList());

        ids.addAll(orderIds);

        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (PmsCancelOrderResponse response : cancelOrderResponseList) {
            if(!response.getCanceled()){
                continue;
            }
            ProjectOrderDetail detail = orderDetailRepository.selectOrderDetail(response.getSourceLineNo(),list.get(0).getProjectId());
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setOrderNum(material.getOrderNum().subtract(detail.getOrderNum()));
            material.setOrderConvertNum(material.getOrderConvertNum().subtract(detail.getOrderConvertNum()));

            ProjectOrderDetail updateDetail = new ProjectOrderDetail();
            //updateDetail.setIsDeleted(1);
            updateDetail.setCancelReason("商家取消:" + response.getMessage());
            updateDetail.setStatus(PmsMaterialStatus.M_CANCELED.getCode());
            updateDetail.setId(detail.getId());
            updateDetail.fillOperationInfo(cancelRequest.getEmployeeCode());
            orderDetailRepository.updateById(updateDetail);
        }
        //3.更新采购单明细/领料单明细 --物料实际下单量、领用量
        //List<Long> orderDetailIds = UtilTool.getListObjectByField(cancelDetailList, "id");
        List<Long> orderDetailIds = cancelDetailList.stream().map(ProjectOrderDetail::getProjectMaterialId).collect(Collectors.toList());
        if(ObjectUtils.isEmpty(orderDetailIds)){
            return;
        }
        EntityWrapper<ProjectPickingDetail> ew_p = new EntityWrapper<>();
        ew_p.in("order_detail_id", orderDetailIds);
        final List<ProjectPickingDetail> cancelPickingDetailList = pickingDetailRepository.selectList(ew_p);
        if (!ObjectUtils.isEmpty(cancelPickingDetailList)) {
            for (ProjectPickingDetail detail : cancelPickingDetailList) {
                ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
                material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));

                ProjectPickingDetail updateDetail = new ProjectPickingDetail();
                updateDetail.setId(detail.getId());
                //updateDetail.setIsDeleted(1);
                updateDetail.setCancelReason("商家取消");
                updateDetail.setStatus(PmsMaterialStatus.M_CANCELED.getCode());
                updateDetail.fillOperationInfo(cancelRequest.getEmployeeCode());
                pickingDetailRepository.updateById(updateDetail);
            }

        }
        //判断更新为什么状态 -- 被取消的物料是不是已测量的，是不是地材的
        updateMaterialStatusByPmsOrderCancel(cancelRequest, materialMap,cancelPickingDetailList);
    }

    private void cancelOrderDataAllByOMS(ProjectOrder order, PmsOrderStateResponse stateResponse) {
        if (order.getStatus().equals(PmsOrderStatus.PO_APPLYING.getCode())) {
            throw new BusinessException("订单正在处理中，请稍后操作!");
        }
        if (order.getStatus().equals(PmsOrderStatus.PO_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }
        final ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());

        //1.更新取消状态
        ProjectOrder updateOrder = new ProjectOrder();
        updateOrder.setId(order.getId());
        updateOrder.setCancelReason(stateResponse.getRemark());
        updateOrder.setStatus(PmsOrderStatus.PO_CANCELED.getCode());
        updateOrder.fillOperationInfo(stateResponse.getOperator());
        orderRepository.updateById(updateOrder);

        //2.更新物料状态和量
        List<Long> ids = Lists.newArrayList();
        List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());

        for(ProjectOrderDetail orderDetail : orderDetailList){
            orderDetail.setCancelReason(stateResponse.getRemark());
            orderDetail.fillOperationInfo(stateResponse.getOperator());
            orderDetailRepository.updateById(orderDetail);
        }

        List<Long> orderIds = UtilTool.getListObjectByField(orderDetailList, "projectMaterialId");
        final ProjectPicking picking = pickingRepository.selectPickingByOrderCode(order.getOrderCode());
        List<ProjectPickingDetail> pickingDetailList = null;
        if (!Objects.isNull(picking)) {
            pickingDetailList = pickingRepository.selectPickingDetail(picking.getId());
            List<Long> pickingIds = UtilTool.getListObjectByField(pickingDetailList, "projectMaterialId");
            ids.addAll(pickingIds);
        }
        ids.addAll(orderIds);

        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (ProjectOrderDetail detail : orderDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setOrderNum(material.getOrderNum().subtract(detail.getOrderNum()));
            material.setOrderConvertNum(material.getOrderConvertNum().subtract(detail.getOrderConvertNum()));
        }
        //3.更新领料单
        if (!Objects.isNull(picking)) {
            ProjectPicking updatePicking = new ProjectPicking();
            updatePicking.fillOperationInfo(stateResponse.getOperator());
            updatePicking.setId(picking.getId());
            updatePicking.setCancelReason(stateResponse.getRemark());
            updatePicking.setStatus(PmsPickingStatus.PP_CANCELED.getCode());
            pickingRepository.updateById(updatePicking);

            updatePickingDetailStatus(stateResponse, picking, materialMap, pickingDetailList);
        }else{
            // TODO: 2019/10/21 若是退厂订单取消，则取消对应的领料单明细
            List<ProjectPickingDetail> projectPickingDetailList = pickingDetailRepository.selectListByOrderCode(order.getOrderCode());
            if(UtilTool.isNotNull(projectPickingDetailList)){
                //查找对应的领料单所有明细信息
                List<ProjectPickingDetail> allPickingDetailList = pickingRepository.selectPickingDetail(projectPickingDetailList.get(0).getPickingId());
                ProjectPicking normalPicking = pickingRepository.selectById(projectPickingDetailList.get(0).getPickingId());
                if(allPickingDetailList.size() == projectPickingDetailList.size()){

                    normalPicking.setCancelReason(stateResponse.getRemark());
                    normalPicking.setStatus(PmsPickingStatus.PP_CANCELED.getCode());
                    normalPicking.fillOperationInfo(stateResponse.getOperator());
                    pickingRepository.updateById(normalPicking);
                }
                updatePickingDetailStatus(stateResponse, normalPicking, materialMap, projectPickingDetailList);
            }
        }
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            //根据物料类别查询是否存在关系数据
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());

            Boolean flag =false;
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(stateResponse.getOperator());

                if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                    //判断物料是否需要测量
                    if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                        if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                            flag=true;
                        }
                    } else {
                        update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                    }
                }else{
                    update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                }
               /* if (material.getPickingNum().compareTo(BigDecimal.ZERO) <= 0) {
                    update.setStatus(PmsMaterialStatus.M_IN_STORAGE.getCode());
                }*/
                materialRepository.updateById(update);


                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode(stateResponse.getOperator());
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName(stateResponse.getOperatorName());
                materialTrace.fillOperationInfo(stateResponse.getOperator());
                traceRepository.insert(materialTrace);
            }

            //查询该类别下的物料
            // updateMaterialTypeStatusToMeasured(order, materialMap);

            updateMaterialTypeStatusToMeasuredByCondition(order, materialMap, flag);
        }
    }

    private void updatePickingDetailStatus(PmsOrderStateResponse stateResponse, ProjectPicking picking, Map<Long, ProjectMaterial> materialMap, List<ProjectPickingDetail> projectPickingDetailList) {
        Long checkId = 0L;
        List<Long> chainIds = Lists.newArrayList();
        for (ProjectPickingDetail detail : projectPickingDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            //更新领料单明细状态
            detail.setStatus(PmsMaterialStatus.M_CANCELED.getCode());

            //更新物料数量
            material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
            material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));

            //查询对应的验收记录
            EntityWrapper<ProjectPickingMaterialChain> entityWrapper = new EntityWrapper<>();
            entityWrapper.where("project_id={0}",picking.getProjectId());
            entityWrapper.where("picking_detail_id = {0}",detail.getId());
            ProjectPickingMaterialChain chain = chainRepository.selectOne(entityWrapper);
            if(!ObjectUtils.isEmpty(chain)){
                ProjectPickingMaterialChain updatechain = new ProjectPickingMaterialChain();
                updatechain.setId(chain.getId());
                updatechain.setStatus(PmsCheckReportStatus.CP_CANCELLED.getCode());
                updatechain.setIsDeleted(IsDeletedStatus.offline.getCode());
                updatechain.fillOperationInfo(stateResponse.getOperator());
                chainRepository.updateById(updatechain);
                checkId = chain.getCheckId();
                chainIds.add(chain.getId());
            }
        }
        if(!ObjectUtils.isEmpty(chainIds)){
            //查询是否存在未签收的
            EntityWrapper<ProjectPickingMaterialChain> ew_c = new EntityWrapper<>();
            ew_c.where("check_id=" + checkId + " and id not in(" + StringUtils.join(chainIds,",") + ")");
            List<ProjectPickingMaterialChain> chainList = chainRepository.selectList(ew_c);
            if(ObjectUtils.isEmpty(chainList)){
                //更新验收记录状态
                MaterialCheckRecord updateRecord = new MaterialCheckRecord();
                updateRecord.setRecordStatus(PmsCheckReportStatus.CP_CANCELLED.getCode());
                updateRecord.setId(checkId);
                updateRecord.setIsDeleted(IsDeletedStatus.offline.getCode());
                checkRecordRepository.updateById(updateRecord);

                LOGGER.info("######[ProjectOrderServiceImpl.cancelOrderDataAllByOMS]更新签收运单状态，运单ID[" + checkId + "] ######");

            }
        }
    }

    private void updateMaterialTypeStatusToMeasuredByCondition(ProjectOrder order, Map<Long, ProjectMaterial> materialMap, Boolean flag) {
        Set<String> types = Sets.newHashSet();
        Collection<ProjectMaterial> materialList = materialMap.values();
        materialList.forEach(material -> types.add(material.getMaterialMiddleType()));
        if(ObjectUtils.isEmpty(types)){
            return;
        }
        EntityWrapper<ProjectMaterialType> ew2 = new EntityWrapper<>();
        ProjectMaterialType mt = new ProjectMaterialType();
        mt.setProjectId(order.getProjectId());
        ew2.setEntity(mt);
        ew2.in("material_type", types);
        final List<ProjectMaterialType> materialTypes = materialTypeRepository.selectList(ew2);
        for (ProjectMaterialType type : materialTypes) {
            //查询该类别下是否还存在没测量的材料
            List<ProjectMaterial> pmList = materialRepository.selectNotMeasureList(type.getMaterialType(),order.getProjectId());
            if(ObjectUtils.isEmpty(pmList) && flag){
                ProjectMaterialType update = new ProjectMaterialType();
                update.setId(type.getId());
                update.fillOperationInfo(order.getCreatedBy());
                if (order.getOrderType().equals(PmsOrderType.PO_AUTO.getCode())) {
                    continue;
                }
                update.setStatus(PmsMaterialTypeStatus.MT_MEASUREED.getCode());
                update.setLastUpdatedTime(new Date());
                materialTypeRepository.updateById(update);
            }else{
                ProjectMaterialType update = new ProjectMaterialType();
                update.setId(type.getId());
                update.fillOperationInfo(order.getCreatedBy());
                if (order.getOrderType().equals(PmsOrderType.PO_AUTO.getCode())) {
                    continue;
                }
                update.setStatus(PmsMaterialTypeStatus.MT_ORIGINAL_STATUS.getCode());
                update.setLastUpdatedTime(new Date());
                materialTypeRepository.updateById(update);
            }
        }
    }

    private void cancelOrderDataByOMS(ProjectOrder order) {
        if (order.getStatus().equals(PmsOrderStatus.PO_APPLYING.getCode())) {
            throw new BusinessException("订单正在处理中，请稍后操作!");
        }
        if (order.getStatus().equals(PmsOrderStatus.PO_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }
        final ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());

        //1.更新取消状态
        ProjectOrder updateOrder = new ProjectOrder();
        updateOrder.setId(order.getId());
        updateOrder.setCancelReason("商家取消");
        updateOrder.setStatus(PmsOrderStatus.PO_CANCELED.getCode());
        updateOrder.fillOperationInfo(info.getProjectManagerCode());
        orderRepository.updateById(updateOrder);

        //2.更新物料状态和量
        List<Long> ids = Lists.newArrayList();
        List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());
        List<Long> orderIds = UtilTool.getListObjectByField(orderDetailList, "projectMaterialId");
        final ProjectPicking picking = pickingRepository.selectPickingByOrderCode(order.getOrderCode());
        List<ProjectPickingDetail> pickingDetailList = null;
        if (!Objects.isNull(picking)) {
            pickingDetailList = pickingRepository.selectPickingDetail(picking.getId());
            List<Long> pickingIds = UtilTool.getListObjectByField(pickingDetailList, "projectMaterialId");
            ids.addAll(pickingIds);
        }
        ids.addAll(orderIds);

        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (ProjectOrderDetail detail : orderDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setOrderNum(material.getOrderNum().subtract(detail.getOrderNum()));
            material.setOrderConvertNum(material.getOrderConvertNum().subtract(detail.getOrderConvertNum()));
        }
        //3.更新领料单
        if (!Objects.isNull(picking)) {
            ProjectPicking updatePicking = new ProjectPicking();
            updatePicking.fillOperationInfo(info.getProjectManagerCode());
            updatePicking.setId(picking.getId());
            updatePicking.setStatus(PmsPickingStatus.PP_CANCELED.getCode());
            pickingRepository.updateById(updatePicking);

            //更新产生的验收数据
            List<MaterialCheckRecord> checkRecordList = checkRecordRepository.selectRecordListByPickingCode(picking.getPickingCode());
            if(!ObjectUtils.isEmpty(checkRecordList)){
                for(MaterialCheckRecord record : checkRecordList){
                    record.setRecordStatus(PmsCheckReportStatus.CP_CANCELLED.getCode());
                    checkRecordRepository.updateById(record);
                }
            }


            for (ProjectPickingDetail detail : pickingDetailList) {
                ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
                material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));
            }
        }
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            //根据物料类别查询是否存在关系数据
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());
            Boolean flag=false;
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(info.getProjectManagerCode());

                if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                    //判断物料是否需要测量
                    if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                        if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                            flag=true;
                        }
                    } else {
                        update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                    }
                }else{
                    update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                }
               /* if (material.getPickingNum().compareTo(BigDecimal.ZERO) <= 0) {
                    update.setStatus(PmsMaterialStatus.M_IN_STORAGE.getCode());
                }*/
                materialRepository.updateById(update);


                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode("admin");
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName("admin");
                materialTrace.fillOperationInfo("admin");
                traceRepository.insert(materialTrace);
            }

            //查询该类别下的物料
            updateMaterialTypeStatusToMeasuredByCondition(order, materialMap,flag);
        }
    }


    private void cancelPmsOrder(ProjectOrder order,ProjectOrderCancelRequest cancelRequest){
        if (order.getStatus().equals(PmsOrderStatus.PO_APPLYING.getCode())) {
            throw new BusinessException("订单正在处理中，请稍后操作!");
        }
        if (order.getStatus().equals(PmsOrderStatus.PO_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }
        final List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());
        //查询是否是远程费订单--
        if(!CollectionUtils.isEmpty(orderDetailList)){
        	for(ProjectOrderDetail detail:orderDetailList){
        		if(remoteFeeSkuCode.equals(detail.getMaterialCode())){
        			throw new BusinessException("项目经理不允许取消远程费订单!");
        		}
        	}
        }

        final ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());
        PmsCancelOrderRequest request = new PmsCancelOrderRequest();
        request.setCause(cancelRequest.getCancelReason());
        request.setOperator(info.getProjectManagerCode());
        request.setOperatorName(info.getProjectManagerName()==null?"admin":info.getProjectManagerName());
        request.setSourceOrderCode(order.getOrderCode());

        List<PmsCancelOrderResponse> cancelOrderResponseList = pmsOrderFeign.pmsCancelOrder(request);

        LOGGER.info("######[ProjectOrderServiceImpl.cancelPmsOrder.cancelOrderResponseList():[" + JSON.toJSONString(cancelOrderResponseList) + "]  ######");


        if(ObjectUtils.isEmpty(cancelOrderResponseList)){
            throw new BusinessException("订单取消失败!");
        }

        List<String> itemIds = cancelOrderResponseList.stream().filter(item->item.getCanceled()==true).map(PmsCancelOrderResponse::getSourceLineNo).collect(Collectors.toList());
        if(ObjectUtils.isEmpty(itemIds)){
            throw new BusinessException("商家已经接单无法取消!");
        }
        EntityWrapper<ProjectOrderDetail> ew = new EntityWrapper<>();
        ew.in("item_id", itemIds);
        final List<ProjectOrderDetail> cancelDetailList = orderDetailRepository.selectList(ew);

        //整单取消
        LOGGER.info("######[ProjectOrderServiceImpl.cancelPmsOrder.orderDetailList.size():[" + orderDetailList.size() + "]  ######");
        LOGGER.info("######[ProjectOrderServiceImpl.cancelPmsOrder.cancelDetailList.size():[" + cancelDetailList.size() + "]  ######");

        if (orderDetailList.size() == cancelDetailList.size()) {
            cancelPmsOrderDataAll(order,cancelRequest);
        } else {
            //部分取消订单
            cancelPmsOrderDataAllPartial(cancelOrderResponseList,cancelDetailList,cancelRequest);
        }

    }

    private void cancelOrderData(ProjectOrder order) {
        if (order.getStatus().equals(PmsOrderStatus.PO_APPLYING.getCode())) {
            throw new BusinessException("订单正在处理中，请稍后操作!");
        }
        if (order.getStatus().equals(PmsOrderStatus.PO_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }
        final ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());
        PmsCancelOrderRequest request = new PmsCancelOrderRequest();
        request.setCause(order.getCancelReason());
        request.setOperator(info.getProjectManagerCode());
        request.setOperatorName(info.getProjectManagerName()==null?"admin":info.getProjectManagerName());
        request.setSourceOrderCode(order.getOrderCode());
        orderInfoFeign.pmsCancelOrder(request);

        //1.更新取消状态
        ProjectOrder updateOrder = new ProjectOrder();
        updateOrder.setId(order.getId());
        updateOrder.setCancelReason(order.getCancelReason());
        updateOrder.setStatus(PmsOrderStatus.PO_CANCELED.getCode());
        updateOrder.fillOperationInfo(info.getProjectManagerCode());
        orderRepository.updateById(updateOrder);

        //2.更新物料状态和量
        List<Long> ids = Lists.newArrayList();
        List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());

        for(ProjectOrderDetail orderDetail : orderDetailList){
            orderDetail.setCancelReason(order.getCancelReason());
            orderDetail.fillOperationInfo(info.getProjectManagerName()==null?"admin":info.getProjectManagerName());
            orderDetailRepository.updateById(orderDetail);
        }

        List<Long> orderIds = UtilTool.getListObjectByField(orderDetailList, "projectMaterialId");
        final ProjectPicking picking = pickingRepository.selectPickingByOrderCode(order.getOrderCode());
        List<ProjectPickingDetail> pickingDetailList = null;
        if (!Objects.isNull(picking)) {
            pickingDetailList = pickingRepository.selectPickingDetail(picking.getId());
            List<Long> pickingIds = UtilTool.getListObjectByField(pickingDetailList, "projectMaterialId");
            ids.addAll(pickingIds);
        }
        ids.addAll(orderIds);

        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (ProjectOrderDetail detail : orderDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setOrderNum(material.getOrderNum().subtract(detail.getOrderNum()));
            material.setOrderConvertNum(material.getOrderConvertNum().subtract(detail.getOrderConvertNum()));
        }
        //3.更新领料单
        if (!Objects.isNull(picking)) {
            ProjectPicking updatePicking = new ProjectPicking();
            updatePicking.fillOperationInfo(info.getProjectManagerCode());
            updatePicking.setId(picking.getId());
            updatePicking.setCancelReason("项目经理取消");
            updatePicking.setStatus(PmsPickingStatus.PP_CANCELED.getCode());
            pickingRepository.updateById(updatePicking);
            for (ProjectPickingDetail detail : pickingDetailList) {
                ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
                material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));
            }
        }
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            //根据物料类别查询是否存在关系数据
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());
            Boolean flag=false;
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(info.getProjectManagerCode());

                if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                    //判断物料是否需要测量
                    if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                        if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                            update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                            flag=true;
                        }
                    } else {
                        update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                    }
                }else{
                    update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                }
                materialRepository.updateById(update);

                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode("admin");
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName("admin");
                materialTrace.fillOperationInfo("admin");
                traceRepository.insert(materialTrace);
            }

            //查询该类别下的物料
            updateMaterialTypeStatusToMeasuredByCondition(order, materialMap,flag);
        }
    }

    private void cancelPickingDataReturn(ProjectPicking order,String msg) {
        LOGGER.info("######[ProjectOrderServiceImpl.cancelPickingDataReturn]领料单返回状态为失败标识，正在取消领料单，领料单号[" + order.getPickingCode() + "],取消原因:[" + msg+"]######");

        if (order.getStatus().equals(PmsPickingStatus.PP_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }
        final ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());

        LOGGER.info("######[ProjectOrderServiceImpl.cancelPickingDataReturn]正在更新领料单状态为失败，领料单单号[" + order.getPickingCode() + "],取消原因:[" + msg+"]######");
        //1.更新取消状态
        ProjectPicking updateOrder = new ProjectPicking();
        updateOrder.setCancelReason(msg);
        updateOrder.setId(order.getId());
        updateOrder.setStatus(PmsPickingStatus.PP_CANCELED.getCode());
        updateOrder.fillOperationInfo(info.getProjectManagerCode());
        pickingRepository.updateById(updateOrder);

        LOGGER.info("######[ProjectOrderServiceImpl.cancelPickingDataReturn]正在更新领料单物料状态和领料量，领料单单号[" + order.getPickingCode() + "],取消原因:[" + msg+"]######");
        //2.更新物料状态和领用量
        List<Long> ids = Lists.newArrayList();
        final ProjectPicking picking = pickingRepository.selectPickingByPickingCode(order.getPickingCode());
        List<ProjectPickingDetail> pickingDetailList = null;
        if (!Objects.isNull(picking)) {
            pickingDetailList = pickingRepository.selectPickingDetail(picking.getId());
            List<Long> pickingIds =pickingDetailList.stream().map(ProjectPickingDetail::getProjectMaterialId).collect(Collectors.toList());
            //List<Long> pickingIds = UtilTool.getListObjectByField(pickingDetailList, "projectMaterialId");
            ids.addAll(pickingIds);
        }
        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (ProjectPickingDetail detail : pickingDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
            material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));
        }
        //更新物料
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(info.getProjectManagerCode());
                update.setStatus(PmsMaterialStatus.M_IN_STORAGE.getCode());
                materialRepository.updateById(update);

                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode("admin");
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName("admin");
                materialTrace.fillOperationInfo("admin");
                traceRepository.insert(materialTrace);
            }

            //更新物料类别状态为已入库状态
            LOGGER.info("######[ProjectOrderServiceImpl.cancelPickingDataReturn]正在更新物料类别状态，领料单单号[" + order.getPickingCode() + "],取消原因:[" + msg+"]######");
            updatMaterialTypeStatusToInstore(order,materialMap);
        }
    }


    private void cancelOrderDataReturn(ProjectOrder order,PmsOrderResponse orderResponse) {
        String msg = orderResponse.getMessage();
        LOGGER.info("######[ProjectOrderServiceImpl.cancelOrderDataReturn]订单返回状态为失败标识，正在取消订单，订单单号[" + order.getOrderCode() + "],取消原因:[" + msg+"]######");

        if (order.getStatus().equals(PmsOrderStatus.PO_CANCELED.getCode())) {
            throw new BusinessException("订单已取消!");
        }
        final ProjectInfo info = projectInfoRepository.selectById(order.getProjectId());

        LOGGER.info("######[ProjectOrderServiceImpl.cancelOrderDataReturn]正在更新订单状态为失败，订单单号[" + order.getOrderCode() + "],取消原因:[" + msg+"]######");
        //1.更新取消状态
        ProjectOrder updateOrder = new ProjectOrder();
        updateOrder.setId(order.getId());
        updateOrder.setStatus(PmsOrderStatus.PO_CANCELED.getCode());
        updateOrder.setCancelReason(orderResponse.getMessage());
        updateOrder.fillOperationInfo(info.getProjectManagerCode());
        orderRepository.updateById(updateOrder);

        LOGGER.info("######[ProjectOrderServiceImpl.cancelOrderDataReturn]正在更新订单物料状态和下单量，订单单号[" + order.getOrderCode() + "],取消原因:[" + msg+"]######");
        //2.更新物料状态和量
        List<Long> ids = Lists.newArrayList();
        List<ProjectOrderDetail> orderDetailList = orderRepository.selectOrderDetail(order.getId());

        for(ProjectOrderDetail orderDetail : orderDetailList){
            orderDetail.setCancelReason(msg);
            orderDetail.fillOperationInfo("admin");
            orderDetailRepository.updateById(orderDetail);
        }

        List<Long> orderIds = UtilTool.getListObjectByField(orderDetailList, "projectMaterialId");
        final ProjectPicking picking = pickingRepository.selectPickingByOrderCode(order.getOrderCode());
        List<ProjectPickingDetail> pickingDetailList = null;
        if (!Objects.isNull(picking)) {
            pickingDetailList = pickingRepository.selectPickingDetail(picking.getId());
            List<Long> pickingIds = UtilTool.getListObjectByField(pickingDetailList, "projectMaterialId");
            ids.addAll(pickingIds);
        }
        ids.addAll(orderIds);

        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        List<ProjectMaterial> list = materialRepository.selectBatchIds(ids);
        if (!ObjectUtils.isEmpty(list)) {
            for (ProjectMaterial material : list) {
                materialMap.put(material.getId(), material);
            }
        }
        for (ProjectOrderDetail detail : orderDetailList) {
            ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
            if (material == null) {
                continue;
            }
            material.setOrderNum(material.getOrderNum().subtract(detail.getOrderNum()));
            material.setOrderConvertNum(material.getOrderConvertNum().subtract(detail.getOrderConvertNum()));
        }
        //3.更新领料单
        if (!Objects.isNull(picking)) {
            LOGGER.info("######[ProjectOrderServiceImpl.cancelOrderDataReturn]正在更新订单对应的领料单，订单单号[" + order.getOrderCode() + "],取消原因:[" + msg+"]######");

            ProjectPicking updatePicking = new ProjectPicking();
            updatePicking.fillOperationInfo(info.getProjectManagerCode());
            updatePicking.setId(picking.getId());
            updatePicking.setStatus(PmsPickingStatus.PP_CANCELED.getCode());
            updatePicking.setCancelReason(msg);
            pickingRepository.updateById(updatePicking);
            for (ProjectPickingDetail detail : pickingDetailList) {
                ProjectMaterial material = materialMap.get(detail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                material.setPickingNum(material.getPickingNum().subtract(detail.getPickingNum()));
                material.setPickingConvertNum(material.getPickingConvertNum().subtract(detail.getPickingConvertNum()));
            }
        }
        //更新物料
        if (!ObjectUtils.isEmpty(materialMap.values())) {
            List<TaskMeterMaterialType> listByTaskType = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            List<String> taskMeterMaterialTypes = listByTaskType.stream().map(TaskMeterMaterialType::getMaterialType).collect(Collectors.toList());
            Boolean flag=false;
            //List<String> taskMeterMaterialTypes = taskMeterMaterialTypeRepository.selectTaskMeterMaterialTypeStrListByTaskType(PmsTaskType.MEASUREMENT.getCode());
            for (ProjectMaterial material : materialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderNum(material.getOrderNum());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setPickingConvertNum(material.getPickingConvertNum());
                update.setPickingNum(material.getPickingNum());
                update.fillOperationInfo(info.getProjectManagerCode());

                if (material.getOrderNum().compareTo(BigDecimal.ZERO) <= 0) {
                    //判断物料是否需要测量
                    if (taskMeterMaterialTypes.contains(material.getMaterialType())) {
                        update.setStatus(PmsMaterialStatus.M_MEASUREED.getCode());
                        flag=true;
                    } else {
                        update.setStatus(PmsMaterialStatus.M_ORIGINAL_STATUS.getCode());
                    }
                }else{
                    update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                }
                materialRepository.updateById(update);

                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode("admin");
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(update.getStatus());
                materialTrace.setFollowUserName("admin");
                materialTrace.fillOperationInfo("admin");
                traceRepository.insert(materialTrace);
            }

            //更新物料类别为已测量
            LOGGER.info("######[ProjectOrderServiceImpl.cancelOrderDataReturn]正在更新物料类别状态，订单单号[" + order.getOrderCode() + "],取消原因:[" + msg+"]######");
            updateMaterialTypeStatusToMeasuredByCondition(order, materialMap,flag);
        }
    }

    private void updatMaterialTypeStatusToInstore(ProjectPicking order,Map<Long, ProjectMaterial> materialMap) {
        Set<String> types = Sets.newHashSet();
        Collection<ProjectMaterial> materialList = materialMap.values();
        materialList.forEach(material -> types.add(material.getMaterialMiddleType()));
        if(ObjectUtils.isEmpty(types)){
            return;
        }
        EntityWrapper<ProjectMaterialType> ew2 = new EntityWrapper<>();
        ProjectMaterialType mt = new ProjectMaterialType();
        mt.setProjectId(order.getProjectId());
        ew2.setEntity(mt);
        ew2.in("material_type", types);
        final List<ProjectMaterialType> materialTypes = materialTypeRepository.selectList(ew2);
        for (ProjectMaterialType type : materialTypes) {
            //查询该类别下是否还存在不入库的材料
            List<ProjectMaterial> pmList = materialRepository.selectNotInStoreList(type.getMaterialType(),order.getProjectId());
            if(ObjectUtils.isEmpty(pmList)){
                ProjectMaterialType update = new ProjectMaterialType();
                update.setId(type.getId());
                update.setStatus(PmsMaterialTypeStatus.MT_IN_STORAGE.getCode());
                update.fillOperationInfo("admin");
                materialTypeRepository.updateById(update);
            }
        }
    }

    private void updateMaterialTypeStatusToMeasured(ProjectOrder order, Map<Long, ProjectMaterial> materialMap) {
        Set<String> types = Sets.newHashSet();
        Collection<ProjectMaterial> materialList = materialMap.values();
        materialList.forEach(material -> types.add(material.getMaterialMiddleType()));
        if(ObjectUtils.isEmpty(types)){
            return;
        }
        EntityWrapper<ProjectMaterialType> ew2 = new EntityWrapper<>();
        ProjectMaterialType mt = new ProjectMaterialType();
        mt.setProjectId(order.getProjectId());
        ew2.setEntity(mt);
        ew2.in("material_type", types);
        final List<ProjectMaterialType> materialTypes = materialTypeRepository.selectList(ew2);
        for (ProjectMaterialType type : materialTypes) {
            //查询该类别下是否还存在没测量的材料
            List<ProjectMaterial> pmList = materialRepository.selectNotMeasureList(type.getMaterialType(),order.getProjectId());
            if(ObjectUtils.isEmpty(pmList)){
                ProjectMaterialType update = new ProjectMaterialType();
                update.setId(type.getId());
                update.fillOperationInfo(order.getCreatedBy());
                if (order.getOrderType().equals(PmsOrderType.PO_AUTO.getCode())) {
                    continue;
                }
                update.setStatus(PmsMaterialTypeStatus.MT_MEASUREED.getCode());
                update.setLastUpdatedTime(new Date());
                materialTypeRepository.updateById(update);
            }
        }
    }

    private SpuUnit getSaleUnitBySkuCode(ProjectMaterial material) {
        SpuUnit spuUnit = null;
        SkuRelationDataVo dataVo = skuFeign.selectSkuRelationDataBySkuCode(material.getMaterialCode());
        List<SpuUnit> spuUnitList = dataVo.getSpuUnitList();
        for (SpuUnit unit : spuUnitList) {
            if (unit.getType().equals(SpuUnitType.SALE_UNIT)) {
                spuUnit = unit;
                break;
            }
        }
        return spuUnit;
    }


    private void updateProjectMaterialStatus(@RequestBody PmsOrderStateResponse orderReturnVo, ProjectInfo info, List<PmsOrderStateDetailResponse> detailVoList, PmsMaterialStatus mDelivered, PmsMaterialTypeStatus mtDelivered) {
        Set<String> typeSet = Sets.newHashSet();
        List<Long> materialIds = Lists.newArrayList();
        ProjectOrder projectOrder = orderRepository.selectOrderByCode(orderReturnVo.getSourceCode());
        for (PmsOrderStateDetailResponse detailVo : detailVoList) {
            //2.更新物料状态
            ProjectMaterial material = materialRepository.selectOneByOrderItemId(detailVo.getLineNo(),projectOrder.getOrderCode(),info.getId());
            material.setStatus(mDelivered.getCode());
            material.fillOperationInfo(orderReturnVo.getOperator());
            materialRepository.updateById(material);

            materialIds.add(material.getId());

            //3.更新订单明细状态
            ProjectOrderDetail orderDetail = orderDetailRepository.selectOrderDetail(detailVo.getLineNo(),info.getId());

            //更新物料类别
            ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(info.getId(), material.getMaterialMiddleType());

            String mtStatus = "";

            //查询该类别所有的物料并且排除当前物料id
            ProjectMaterial param = new ProjectMaterial();
            param.setIsDeleted(0);
            param.setProjectId(info.getId());
            param.setMaterialMiddleType(material.getMaterialMiddleType());
            EntityWrapper<ProjectMaterial> ew = new EntityWrapper<>(param);

            if (!Objects.isNull(orderDetail)) {
                if(mDelivered.getCode().equals(PmsMaterialStatus.M_ORDER_CONFIRMED.getCode())){
                    if(!orderDetail.getStatus().equals(PmsMaterialStatus.M_ORDERED.getCode())){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单还未提交成功,行号：[" + orderDetail.getItemId() + "]######");
                        throw new BusinessException("######状态更新失败，当前订单还未提交成功,行号：[" + orderDetail.getItemId() + "]######");
                    }
                    ew.where(" ID NOT IN (" + StringUtils.join(materialIds, ",") + ") and status in('mOriginalStatus','mMeasured','mOrdered')");
                    mtStatus = PmsMaterialTypeStatus.MT_ORDER_CONFIRMED.getCode();
                }
                if(mDelivered.getCode().equals(PmsMaterialStatus.M_ORDER_SEND_CONFIRMED.getCode())){
                    if(orderDetail.getStatus().equals(PmsMaterialStatus.M_ORDER_SEND_CONFIRMED.getCode())){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单已发货确认成功,行号：[" + orderDetail.getItemId() + "]######");
                        continue;
                    }
                    if(orderDetail.getStatus().equals(PmsMaterialStatus.M_IN_STORAGE.getCode())){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单已入库成功,行号：[" + orderDetail.getItemId() + "]######");
                        continue;
                    }
                    if(orderDetail.getStatus().equals(PmsMaterialStatus.M_DELIVERED.getCode())){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单已配送成功,行号：[" + orderDetail.getItemId() + "]######");
                        continue;
                    }
                    if(orderDetail.getStatus().equals(PmsMaterialStatus.M_RECEIVED.getCode())){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单已收货确认成功,行号：[" + orderDetail.getItemId() + "]######");
                        continue;
                    }
                    if(!orderDetail.getStatus().equals(PmsMaterialStatus.M_ORDER_CONFIRMED.getCode())){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单还未确认成功,行号：[" + orderDetail.getItemId() + "]######");
                        throw new BusinessException("######状态更新失败，当前订单还未确认成功,行号：[" + orderDetail.getItemId() + "]######");
                    }
                    //更新订单明细状态为已领料
                    if(orderReturnVo.getState().equals("3") &&  (!ObjectUtils.isEmpty(orderDetail.getInStore()) && orderDetail.getInStore())){
                        orderDetail.setStatus(PmsMaterialStatus.M_PICKED.getCode());

                        material.setStatus(PmsMaterialStatus.M_PICKED.getCode());
                        materialRepository.updateById(material);


                        ew.where(" ID NOT IN (" + StringUtils.join(materialIds, ",") + ") and status in('mOriginalStatus','mMeasured','mOrdered','mOrderConfirmed','mOrderSendConfirmed','mInStorage')");
                        mtStatus = PmsMaterialTypeStatus.MT_PICKED.getCode();


                    }else{
                        mtStatus = PmsMaterialTypeStatus.MT_ORDER_SEND_CONFIRMED.getCode();
                        ew.where(" ID NOT IN (" + StringUtils.join(materialIds, ",") + ") and status in('mOriginalStatus','mMeasured','mOrdered','mOrderConfirmed')");
                    }
                    updatePickingDetailStatusByOrderStatus(orderReturnVo, mDelivered, orderDetail);
                }
                if(mDelivered.getCode().equals(PmsMaterialStatus.M_IN_STORAGE.getCode())){

                    if(!orderDetail.getStatus().equals(PmsMaterialStatus.M_ORDER_SEND_CONFIRMED.getCode())
                            && !orderDetail.getStatus().equals(PmsMaterialStatus.M_IN_STORAGE.getCode())
                            ){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单还未发货确认成功,行号：[" + orderDetail.getItemId() + "]######");
                        throw new BusinessException("######状态更新失败，当前订单还未发货确认成功,行号：[" + orderDetail.getItemId() + "]######");
                    }

                    //更新订单状态为已完成
                    updateOrderStatusToCompleted(orderReturnVo, orderDetail);
                    //更新对应的领料单状态
                    updatePickingDetailStatusByOrderStatus(orderReturnVo, mDelivered, orderDetail);
                    //更新类别状态
                    ew.where(" ID NOT IN (" + StringUtils.join(materialIds, ",") + ") and status in('mOriginalStatus','mMeasured','mOrdered','mOrderConfirmed','mOrderSendConfirmed')");
                    mtStatus = PmsMaterialTypeStatus.MT_IN_STORAGE.getCode();
                }
                if(mDelivered.getCode().equals(PmsMaterialStatus.M_DELIVERED.getCode())){
                    if(!orderDetail.getStatus().equals(PmsMaterialStatus.M_PICKED.getCode())){
                        LOGGER.error("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]状态更新失败，当前订单还未入库成功,行号：[" + orderDetail.getItemId() + "]######");
                        //throw new BusinessException("######状态更新失败，当前订单还未入库成功,行号：[" + orderDetail.getItemId() + "]######");
                    }
                    updatePickingDetailStatusByOrderStatus(orderReturnVo, mDelivered, orderDetail);
                    //更新类别状态
                    ew.where(" ID NOT IN (" + StringUtils.join(materialIds, ",") + ") and status in('mOriginalStatus','mMeasured','mOrdered','mOrderConfirmed','mOrderSendConfirmed','mInStorage','mPicked')");
                    mtStatus = PmsMaterialTypeStatus.MT_DELIVERED.getCode();
                }

                //如果是地才的则更新为picked
                if(orderReturnVo.getState().equals("3") &&  (!ObjectUtils.isEmpty(orderDetail.getInStore()) && orderDetail.getInStore())){
                    //orderDetail.setStatus(PmsMaterialStatus.M_PICKED.getCode());

                    //material.setStatus(PmsMaterialStatus.M_PICKED.getCode());
                    //materialRepository.updateById(material);
                }else{
                    orderDetail.setStatus(mDelivered.getCode());
                }

                orderDetailRepository.updateById(orderDetail);

                //查询订单是否还存在未确认未入库未
                if(mDelivered.getCode().equals(PmsMaterialStatus.M_IN_STORAGE.getCode())) {
                    //updateOrderStatusToCompleted(orderReturnVo, orderDetail);
                }

                List<ProjectMaterial> materialList = materialRepository.selectList(ew);
                if (ObjectUtils.isEmpty(materialList)) {
                    if(ObjectUtils.isEmpty(materialType)){
                        continue;
                    }
                    ProjectMaterialType updateType = new ProjectMaterialType();
                    updateType.setId(materialType.getId());
                    updateType.setStatus(mtStatus);
                    updateType.fillOperationInfo(orderReturnVo.getOperator());
                    materialTypeRepository.updateById(updateType);
                }
            }
            if(!mDelivered.getCode().equals(PmsMaterialStatus.M_ORDER_CONFIRMED.getCode())){
               // updatePickingDetailStatusByOrderStatus(orderReturnVo, mDelivered, orderDetail);
            }

            //4.插入状态记录表
            ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
            materialTrace.setFollowUserCode(orderReturnVo.getOperator());
            materialTrace.setProjectId(info.getId());
            materialTrace.setProjectMaterialId(material.getId());
            materialTrace.setFollowTime(orderReturnVo.getOperateTime());
            materialTrace.setStatus(mDelivered.getCode());
            materialTrace.setFollowUserName(orderReturnVo.getOperatorName());
            materialTrace.fillOperationInfo(orderReturnVo.getOperator());
            traceRepository.insert(materialTrace);

            typeSet.add(material.getMaterialMiddleType());

            //更新物料类别状态

        }

        //5.更新物料类别状态
//        for (String type : typeSet) {
//            ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(info.getId(), type);
//
//            //查询该类别所有的物料并且排除当前物料id
//            ProjectMaterial param = new ProjectMaterial();
//            param.setStatus(mDelivered.getCode());
//            param.setProjectId(info.getId());
//            param.setMaterialMiddleType(type);
//            EntityWrapper<ProjectMaterial> ew = new EntityWrapper<>(param);
//            ew.where(" ID NOT IN (" + StringUtils.join(materialIds, ",") + ")");
//
//            List<ProjectMaterial> materialList = materialRepository.selectList(ew);
//            if (ObjectUtils.isEmpty(materialList)) {
//                if(ObjectUtils.isEmpty(materialType)){
//                    continue;
//                }
//                ProjectMaterialType updateType = new ProjectMaterialType();
//                updateType.setId(materialType.getId());
//                updateType.setStatus(mtDelivered.getCode());
//                materialTypeRepository.updateById(updateType);
//            }
//        }
    }

    private void updateOrderStatusToCompleted(@RequestBody PmsOrderStateResponse orderReturnVo, ProjectOrderDetail orderDetail) {
        EntityWrapper<ProjectOrderDetail> entityWrapper = new EntityWrapper<>();
        ProjectOrderDetail entity = new ProjectOrderDetail();
        entity.setOrderId(orderDetail.getOrderId());
        entityWrapper.setEntity(entity);
        entityWrapper.where("status in('mOrdered','mOrderConfirmed','mOrderSendConfirmed')");
        List<ProjectOrderDetail> orderDetailList = orderDetailRepository.selectList(entityWrapper);
        if (ObjectUtils.isEmpty(orderDetailList)) {
            ProjectOrder updateOrder = new ProjectOrder();
            updateOrder.setId(orderDetail.getOrderId());
            updateOrder.fillOperationInfo(orderReturnVo.getOperator());
            updateOrder.setStatus(PmsOrderStatus.PO_COMPLETED.getCode());
            orderRepository.updateById(updateOrder);
        }
    }

    private void updatePickingDetailStatusByOrderStatus(@RequestBody PmsOrderStateResponse orderReturnVo, PmsMaterialStatus mDelivered, ProjectOrderDetail orderDetail) {
        //LOGGER.info("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]查询对应的领料明细[" + orderDetail.getItemId() + "]start######");
        ProjectPickingDetail pickingDetail = pickingDetailRepository.selectOneByOrderDetailId(orderDetail.getId(),orderDetail.getProjectId());
        //LOGGER.info("######[ProjectOrderServiceImpl.updateProjectMaterialStatus]查询对应的领料明细[" + orderDetail.getItemId() + "]end######");
        if (!Objects.isNull(pickingDetail)) {
            //如果是地才的则更新为picked
            if(orderReturnVo.getState().equals("3")){
                pickingDetail.setStatus(PmsMaterialStatus.M_PICKED.getCode());
            }else {
                pickingDetail.setStatus(mDelivered.getCode());
            }
            pickingDetailRepository.updateById(pickingDetail);

            //查询订单是否还存在未确认未入库未
            if(mDelivered.getCode().equals(PmsMaterialStatus.M_IN_STORAGE.getCode())) {
                EntityWrapper<ProjectPickingDetail> entityWrapper = new EntityWrapper<>();
                ProjectPickingDetail entity = new ProjectPickingDetail();
                entity.setPickingId(orderDetail.getOrderId());
                entityWrapper.setEntity(entity);
                entityWrapper.where("status in('mOrdered','mOrderConfirmed','mOrderSendConfirmed')");
                List<ProjectPickingDetail> orderDetailList = pickingDetailRepository.selectList(entityWrapper);
                if (!ObjectUtils.isEmpty(orderDetailList)) {
                    ProjectPicking updatePicking = new ProjectPicking();
                    updatePicking.setId(pickingDetail.getPickingId());
                    updatePicking.fillOperationInfo(orderReturnVo.getOperator());
                    updatePicking.setStatus(PmsPickingStatus.PP_COMPLETED.getCode());
                    pickingRepository.updateById(updatePicking);
                }
            }
        }
    }


    private void buildAutoPickingAndDetailList(@RequestBody PmsOrderStateResponse orderReturnVo, ProjectInfo info, List<PmsOrderStateDetailResponse> detailVoList) {
        List<ProjectPickingDetail> pickingDetailList = Lists.newArrayList();
        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        for (PmsOrderStateDetailResponse detailVo : detailVoList) {
            ProjectOrderDetail orderDetail = orderDetailRepository.selectOrderDetail(detailVo.getLineNo(),info.getId());
            ProjectMaterial material = materialMap.get(orderDetail.getProjectMaterialId());
            if(ObjectUtils.isEmpty(material)){
                material = materialRepository.selectById(orderDetail.getProjectMaterialId());
                materialMap.put(material.getId(), material);
            }

            //不入库的会生成领料明细
            if (!ObjectUtils.isEmpty(orderDetail.getInStore()) && orderDetail.getInStore()) {
                //查询领料明细是否存在，存在则更新
                ProjectPickingDetail pickingDetail = pickingDetailRepository.selectOneByOrderDetailId(orderDetail.getId(),info.getId());
                if(ObjectUtils.isEmpty(pickingDetail)){
                    ProjectPickingDetail detail = new ProjectPickingDetail();
                    detail.setStatus(PmsMaterialStatus.M_ORDER_CONFIRMED.getCode());
                    detail.setProjectId(info.getId());
                    detail.setProjectMaterialId(orderDetail.getProjectMaterialId());
                    detail.setSpecialCode(orderDetail.getSpecialCode());
                    detail.setMaterialCode(orderDetail.getMaterialCode());
                    detail.setMaterialName(orderDetail.getMaterialName());
                    detail.setPickingNum(orderDetail.getOrderNum());
                    detail.setProManagerPrice(orderDetail.getProManagerPrice());
                    detail.setPickingConvertNum(orderDetail.getOrderConvertNum());
                    detail.setSaleUnit(orderDetail.getSaleUnit());
                    detail.setConvertUnit(orderDetail.getConvertUnit());
                    detail.setRemark(orderDetail.getRemark());
                    detail.setOrderDetailId(orderDetail.getId());
                    detail.setOrderCode(orderReturnVo.getSourceCode());
                    detail.setOmsOrderCode(orderDetail.getOmsOrderCode());
                    detail.fillOperationInfo(orderReturnVo.getOperator());
                    pickingDetailList.add(detail);

                    material.setPickingNum((material.getPickingNum() == null?BigDecimal.ZERO:material.getPickingNum().add(orderDetail.getOrderNum())));
                    material.setPickingConvertNum((material.getPickingConvertNum() == null?BigDecimal.ZERO:material.getPickingConvertNum().add(orderDetail.getOrderConvertNum())));

                }else{
                    pickingDetail.setStatus(PmsMaterialStatus.M_ORDER_CONFIRMED.getCode());
                    pickingDetail.fillOperationInfo(orderReturnVo.getOperator());
                    pickingDetailRepository.updateById(pickingDetail);
                }

            }

            //商家接受订单更新计划任务
            /*ProjectPlanTask projectPlanTask = planTaskRepository.selectOneByTypeAndProjectId(material.getProjectId(), material.getMaterialType(), PmsTaskType.ORDER.getCode());
            if (!ObjectUtils.isEmpty(projectPlanTask)) {
                projectPlanTask.setActualEndDate(new Date());
                projectPlanTask.setStatus(ProjectTaskStatus.TCompleted.getCode());
                projectPlanTask.fillOperationInfo(orderReturnVo.getOperator());
                planTaskRepository.updateById(projectPlanTask);
            }*/
        }
        if (ObjectUtils.isEmpty(pickingDetailList)) {
            return;
        }
        ProjectPicking picking = new ProjectPicking();
        ProjectPicking existPicking = pickingRepository.selectPickingByOrderCode(orderReturnVo.getSourceCode());
        if(ObjectUtils.isEmpty(existPicking)){
            picking.setStatus(PmsPickingStatus.PP_SUBMITTED.getCode());
            picking.setPickingType(PmsPickingType.PP_AUTO.getCode());
            picking.setOrderCode(orderReturnVo.getSourceCode());
            picking.setPickingCode(codeRuleFeign.generateCode(RuleCode.PMS_PICKING, info.getCompanyCode().substring(0, 3)));
            picking.setProjectId(info.getId());
            picking.setRemark("订单确认产生");
            picking.fillOperationInfo(orderReturnVo.getOperator());
            pickingRepository.insert(picking);
        }else{
            picking = existPicking;
        }

        //更新行号
        for (ProjectPickingDetail detail : pickingDetailList) {
            detail.setPickingId(picking.getId());
        }
        pickingDetailRepository.insertBatch(pickingDetailList);
        if(!ObjectUtils.isEmpty(materialMap.values())){
            for(ProjectMaterial projectMaterial : materialMap.values()){
                ProjectMaterial update = new ProjectMaterial();
                update.setPickingConvertNum(projectMaterial.getPickingConvertNum());
                update.setPickingNum(projectMaterial.getPickingNum());
                update.setId(projectMaterial.getId());
                update.setStatus(PmsMaterialStatus.M_ORDER_CONFIRMED.getCode());
                materialRepository.updateById(update);
            }

        }


        for (ProjectPickingDetail detail : pickingDetailList) {
            detail.setItemId(detail.getId().toString());
            pickingDetailRepository.updateById(detail);
        }

    }


    /**
     * 更新主材表数据下单领料
     *
     * @param details
     */
    private void updateProjectMaterial(List<ProjectOrderDetail> details, ProjectOrderRequestVo request, List<Long> newMaterialList,Map<Long, ProjectMaterial> updateMaterialMap) {
        List<Long> ids = Lists.newArrayList();
        List<Long> orderIds = details.stream().map(ProjectOrderDetail::getProjectMaterialId).collect(Collectors.toList());
        ids.addAll(orderIds);
        if (!ObjectUtils.isEmpty(details)) {
            for (ProjectOrderDetail detail : details) {
                if (newMaterialList.contains(detail.getProjectMaterialId())) {
                    continue;
                }
                ProjectMaterial material = updateMaterialMap.get(detail.getProjectMaterialId());
                if (material == null) {
                    continue;
                }
                material.setOrderConvertNum((material.getOrderConvertNum() == null ? BigDecimal.ZERO : material.getOrderConvertNum()).add(detail.getOrderConvertNum()));
                material.setOrderNum((material.getOrderNum() == null ? BigDecimal.ZERO : material.getOrderNum()).add(detail.getOrderNum()));
            }
        }
        if (!ObjectUtils.isEmpty(updateMaterialMap.values())) {
            for (ProjectMaterial material : updateMaterialMap.values()) {
                ProjectMaterial update = new ProjectMaterial();
                update.setId(material.getId());
                update.setOrderConvertNum(material.getOrderConvertNum());
                update.setOrderNum(material.getOrderNum());
                update.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                update.fillOperationInfo(request.getEmployeeCode());
                materialRepository.updateById(update);

                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode(request.getEmployeeCode());
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                materialTrace.setFollowUserName(request.getEmployeeCode());
                materialTrace.fillOperationInfo(request.getEmployeeCode());
                traceRepository.insert(materialTrace);
            }
        }
    }

    /**
     * @param companyCode
     * @return
     */
    private StoreLocation findStoreLocation(String companyCode) {
        List<StoreLocation> locationList = storeLocationFeign.findLocationByCompany(companyCode);
        if (ObjectUtils.isEmpty(locationList)) {
            LOGGER.info("######[ProjectPickingServiceImpl.findStoreLocation]分公司%s没有维护对应的库存地：[" +  companyCode + "]######");
            throw new InvalidParameterException("分公司%s没有维护对应的库存地", companyCode);
        }
        return locationList.get(0);
    }


    /**
     * 构建以skucode为key的采购价格
     */
    private Map<String, PurchasePrice> buildSkuPriceMap(List<String> skuCodes, Long locationId) {
        List<PurchasePrice> prices = purchasePriceFeign.findPriceBySkuAndLocation(locationId, skuCodes,null);
        Map<String, PurchasePrice> results = new HashMap<>();
        for (PurchasePrice price : prices) {
            if (results.containsKey(price.getSkuCode())) {// 存在多个采购价，系统不允许通过，若允许该情况，需提供规则
                //throw new InvalidParameterException("物料[%s]存在多个采购价，系统无法判定价格", price.getSkuCode());
            }
            results.put(price.getSkuCode(), price);
        }
        return results;
    }

    public Map<String,BidPmsPriceVo> addProjectRemoteFeeOrder(Long projectId,String skuCode) {
        ProjectInfo info = projectInfoRepository.selectById(projectId);
        if(ObjectUtils.isEmpty(info)){
            throw new BusinessException("参数有误，项目不存在，id:[" + projectId + "]");
        }
        //拆分明细
        //String skuCode = "1201100200550000010001";
        validateRemoteFeeSku(info, skuCode);
        //报价系统获取项目经理价
        BidPmsPriceRequestParam bidPmsPriceRequestParam = new BidPmsPriceRequestParam();
        List<BidPmsSkuCodeVo> skuCodeVos = Lists.newArrayList();
        BidPmsSkuCodeVo skuCodeVo = new BidPmsSkuCodeVo();
        skuCodeVo.setSkuCode(skuCode);
        skuCodeVos.add(skuCodeVo);

        bidPmsPriceRequestParam.setPriceType("projectManagerPrice");
        bidPmsPriceRequestParam.setSkuCodes(skuCodeVos);
        bidPmsPriceRequestParam.setBizopId(info.getTrackId());
        Map<String,BidPmsPriceVo>  priceVoMap = bidPmsFeign.getPrice(bidPmsPriceRequestParam);
        return priceVoMap;
        /*if (true) {
            ProjectMaterial material = new ProjectMaterial();
            material.setProjectId(projectId);
            //material.setMaterialType(detailParam.getMaterialType());
            material.setStatus(PmsMaterialStatus.M_PICKED.getCode());
            material.setAddFlag(true);
            SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(skuCode);
            if (skuRelationDataVo != null) {
                //采购单位 //销售单位
                List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                //获取采购单位和销售单位根据类型
                if (!ObjectUtils.isEmpty(spuUnitList)) {
                    for (SpuUnit unit : spuUnitList) {
                        if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                            material.setSaleUnit(unit.getUnitName());
                        }
                        if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                            material.setConvertUnit(unit.getUnitName());
                        }
                    }
                }
            }

            Category category = categoryFeign.getById(skuRelationDataVo.getSpu().getCategoryId());
            Category middleCategory = categoryFeign.getById(category.getParentId());
            //小类
            material.setMaterialType(category.getCode());
            material.setMaterialTypeName(category.getName());
            //中类
            material.setMaterialMiddleType(middleCategory.getCode());
            material.setMaterialMiddleTypeName(middleCategory.getName());

            material.setMaterialCode(skuCode);
            material.setMaterialName(skuRelationDataVo.getSku().getDescription());
            //特项代码
            material.setSpecialCode(skuRelationDataVo.getSpu().getSpecialCode());

            if(ObjectUtils.isEmpty(priceVoMap.get(skuCode))){
               throw new BusinessException("sku[" + skuCode + "]未维护项目经理价格，请联系基础数据部门!");
            }else{
                material.setProManagerPrice(priceVoMap.get(skuCode).getPrice());
            }
            material.fillOperationInfo("admin");
            materialRepository.insert(material);

            ProjectMaterialType mt = materialTypeRepository.selectOneByTypeAndProjectId(projectId,material.getMaterialMiddleType());
            if(ObjectUtils.isEmpty(mt)){
                ProjectMaterialType materialType = new ProjectMaterialType();
                materialType.setProjectId(projectId);
                materialType.setIsDeleted(0);
                materialType.setMaterialType(material.getMaterialMiddleType());
                materialType.setMaterialTypeName(material.getMaterialMiddleTypeName());
                materialType.fillOperationInfo("admin");
                materialTypeRepository.insert(materialType);
            }

            //包装下单明细
            List<ProjectOrderDetailResquest> orderDetailResquestList = Lists.newArrayList();
            ProjectOrderDetailResquest orderDetail = new ProjectOrderDetailResquest();
            orderDetail.setMaterialCode(material.getMaterialCode());
            orderDetail.setMaterialName(material.getMaterialName());
            //项目经理价
            orderDetail.setProManagerPrice(material.getProManagerPrice().stripTrailingZeros().toPlainString());
            orderDetail.setRemark("远程费");
            //计划量
            //采购单位数量
            orderDetail.setOrderNum(planNum.stripTrailingZeros().toEngineeringString());

            if (skuRelationDataVo != null) {
                //采购单位 //销售单位
                List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                //获取采购单位和销售单位根据类型
                if (!ObjectUtils.isEmpty(spuUnitList)) {
                    for (SpuUnit unit : spuUnitList) {
                        if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                            BigDecimal convertNum = planNum.multiply(unit.getConvertRate()).setScale(unit.getDecimalLength(),BigDecimal.ROUND_HALF_UP);
                            orderDetail.setOrderConvertNum(convertNum.stripTrailingZeros().toPlainString());
                        }
                        if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                            orderDetail.setConvertUnit(unit.getUnitName());
                        }
                    }
                }
            }
            orderDetail.setProManagerPrice(material.getProManagerPrice().stripTrailingZeros().toEngineeringString());
            orderDetail.setProjectMaterialId(material.getId());
            orderDetail.setProjectId(material.getProjectId());
            orderDetail.setRemark("");
            orderDetail.setVendorCode(vendorCode);
            orderDetailResquestList.add(orderDetail);
            if(ObjectUtils.isEmpty(orderDetailResquestList)){
                throw new BusinessException("订单明细为空，不能提交，项目ID：[" + info.getId() + "]");
            }
            //包装下单明细
            ProjectOrderRequestVo request = new ProjectOrderRequestVo();
            request.setEmployeeCode(operator);
            request.setProjectId(info.getId());
            request.setEmployeeName(operatorName);
            request.setEmergencyFlag(0);
            request.setMaterialTypeCode(material.getMaterialMiddleType());
            request.setMaterialTypeName(material.getMaterialMiddleTypeName());
            request.setOrderDetailList(orderDetailResquestList);
            this.addProjectOrder(request);

        }*/
    }

    //@Transactional(propagation= Propagation.REQUIRES_NEW)
    public void insertProjectOrder(List<ProjectOrderDetailResquest> list,String operator,String operatorName,Long projectId,String middleType,String middleTypeName){
        ProjectOrderRequestVo request = new ProjectOrderRequestVo();
        request.setEmployeeCode(operator);
        request.setProjectId(projectId);
        request.setEmployeeName(operatorName);
        request.setEmergencyFlag(0);
        request.setMaterialTypeCode(middleType);
        request.setMaterialTypeName(middleTypeName);
        request.setOrderDetailList(list);
        this.addProjectOrder(request);
    }

    private void validateRemoteFeeSku(ProjectInfo info, String skuCode) {
        Map<String, PurchasePrice> skuPriceMap = new HashMap<>();
        Set<String> skuCodeSet = Sets.newHashSet();
        skuCodeSet.add(skuCode);
        List<String> skuCodes = new ArrayList<>(skuCodeSet);
        StoreLocation location = findStoreLocation(info.getCompanyCode());
        skuPriceMap = buildSkuPriceMap(skuCodes, location.getId());
        //如果该物料属性是备货的则不显示
        // 设置明细供应商属性
        PurchasePrice price = skuPriceMap.get(skuCode);
        if(price==null){
            LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku采购价为空需要过滤：[" + skuCode + "######");
            throw new BusinessException("来源行%s物料采购价格未找到", skuCode);
        }
        // 设置明细是否入库属性
        Inventory inventory = inventoryFeign.findInventory(skuCode, price.getVendorCode()
                , location.getCode());
        if (inventory == null) {
            LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku库存属性为空需要过滤：[" + skuCode + "######");

            throw new BusinessException("来源行%s物料库存属性为空", skuCode);
        }
        if(inventory.getPreStoreFlag()!=null&&inventory.getPreStoreFlag()){
            LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku库存属性为备货的需要过滤：[" + skuCode + "######");
            throw new BusinessException("来源行%s物料库存属性为备货，不能下单", skuCode);
        }
    }

    /**
     * 校验开工后增减是否收款
     * @param detailResquestList
     */
    void checkIncreaseMaterialFundReceive(List<ProjectOrderDetailResquest> detailResquestList){
        List<Long> ids = detailResquestList.stream().map(ProjectOrderDetailResquest::getProjectMaterialId).collect(Collectors.toList());
        List<ProjectMaterial> materialList = materialRepository.selectBatchIds(ids);
        if(ObjectUtils.isEmpty(materialList)){
            return;
        }
        List<Long> comfirmIds = materialList.stream().filter(item->item.getComfirmDetailId()!=null).map(ProjectMaterial::getComfirmDetailId).collect(Collectors.toList());
        if(ObjectUtils.isEmpty(comfirmIds)){
            return;
        }
        boolean flag = bidPmsFeign.selectFinanceFlag(comfirmIds);
        if(!flag){
            throw new BusinessException("当前材料中存在开工后未交款的补充合同，请通知客户交款");
        }
    }

    /**
     * 校验开工后增减是否收款
     * @param detailResquestList
     */
    @Override
    public void checkIncreaseMaterialFundReceiveTest(@RequestBody List<Long> detailResquestList){

        List<ProjectMaterial> materialList = materialRepository.selectBatchIds(detailResquestList);
        List<Long> comfirmIds = materialList.stream().filter(item->item.getComfirmDetailId()!=null).map(ProjectMaterial::getComfirmDetailId).collect(Collectors.toList());
        boolean flag = bidPmsFeign.selectFinanceFlag(comfirmIds);
        if(!flag){
            throw new BusinessException("当前材料中存在开工后未交款的补充合同，请通知客户交款");
        }
    }

    private void buildOrderDetailUnitInfo(ProjectOrderDetailResquest orderDetail, BigDecimal planNum, SkuRelationDataVo skuRelationDataVo) {
        if (skuRelationDataVo != null) {
            //采购单位 //销售单位
            List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
            //获取采购单位和销售单位根据类型
            if (!ObjectUtils.isEmpty(spuUnitList)) {
                for (SpuUnit unit : spuUnitList) {
                    if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                        BigDecimal convertNum = planNum.multiply(unit.getConvertRate()).setScale(unit.getDecimalLength(),BigDecimal.ROUND_HALF_UP);
                        orderDetail.setOrderConvertNum(convertNum.stripTrailingZeros().toPlainString());
                    }
                    if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                        orderDetail.setConvertUnit(unit.getUnitName());
                    }
                }
            }
        }
    }

    private void checkSkuInventoryExisted(List<ProjectMaterial> materialList, Map<String, PurchasePrice> skuPriceMap, StoreLocation location) {
        for(ProjectMaterial material : materialList){
            // 设置明细供应商属性
            PurchasePrice price = skuPriceMap.get(material.getMaterialCode());
            if(price==null){
                LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku采购价为空需要过滤：[" + material.getMaterialCode() + "######");
                //continue;
                throw new BusinessException("来源行%s物料采购价格未找到", material.getMaterialCode());
            }
            // 设置明细是否入库属性
            Inventory inventory = inventoryFeign.findInventory(material.getMaterialCode(), price.getVendorCode()
                    , location.getCode());
            if (inventory == null) {
                LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku库存属性为空需要过滤：[" + material.getMaterialCode() + "######");
                //continue;
                throw new BusinessException("来源行%s物料库存属性为空", material.getMaterialCode());
            }
            if(inventory.getPreStoreFlag()){
                LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku库存属性为备货的需要过滤：[" + material.getMaterialCode() + "######");
                continue;
            }
        }
    }

    private void checkNewSkuInventoryExisted(List<ProjectMaterial> materialList, Map<String,ThreadSkuDataInfoPrice> skuDataInfoPriceMap) {
        for(ProjectMaterial material : materialList){
            // 设置明细供应商属性
            //PurchasePrice price = skuPriceMap.get(material.getMaterialCode());
            ThreadSkuDataInfoPrice price = skuDataInfoPriceMap.get(material.getMaterialCode());
            if(price==null){
                LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku采购价为空需要过滤：[" + material.getMaterialCode() + "######");
                //continue;
                throw new BusinessException("来源行%s物料采购价格未找到", material.getMaterialCode());
            }
            // 设置明细是否入库属性
            //Inventory inventory = inventoryFeign.findInventory(material.getMaterialCode(), price.getVendorCode(), location.getCode());
            ThreadSkuDataInfoPrice inventory = skuDataInfoPriceMap.get(material.getMaterialCode());
            if (inventory == null) {
                LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku库存属性为空需要过滤：[" + material.getMaterialCode() + "######");
                //continue;
                throw new BusinessException("来源行%s物料库存属性为空", material.getMaterialCode());
            }
            if(UtilTool.isNotNull(inventory.getPreStoreFlag()) && inventory.getPreStoreFlag()){
                LOGGER.info("######[PmsSoftStartWorkApplyBean.designerStartWorkOrderedDelegate]sku库存属性为备货的需要过滤：[" + material.getMaterialCode() + "######");
                continue;
            }

        }
    }

    private void sendMsgToDesigner(String cancelReason, ProjectInfo projectInfo) {
        try{

            // 校验通过之后, 准备调用短信接口; 需要准备短信内容(来源于配置的短信模板), 手机号
            Sms sms = new Sms();
            List<String> phoneNumList = new ArrayList<>();
            phoneNumList.add(projectInfo.getProjectManagerPhone());
            String smsContent = "尊敬的管家：" + projectInfo.getProjectManagerName() +",您的工地：客户[" + projectInfo.getCustomerName() + "],地址：["+projectInfo.getProjectName() +"]商家取消订单,取消原因：[" + cancelReason +"],请您及时关注!";
            // 1. 设置发送的目标手机号hi
            sms.setPhoneNumList(phoneNumList);
            // 2. 设置短信内容
            sms.setSmsContent(smsContent);
            // 3. 发送短信
            Map<String, Object> returnMsg = smsClient.sendMessageBatch(sms);
            // 获取返回值： 返回值大于0, 就代表发送成功; 否则发送失败。
            String returnVal = (String)returnMsg.get("returnVal");
            Integer valInt = Integer.valueOf(returnVal);
            if(valInt>0){
                LOGGER.info("======工地订单退回：【" + projectInfo.getProjectManagerName()  + "]商机ID:【" + projectInfo.getTrackId()+"】");
            }
        }catch (Exception e){
            LOGGER.info("======工地订单退回发送短信失败");
        }
    }

    private void updateOrderPaymentSource(ProjectExcessOrderRequestVo paramVo,
                                          ProjectOrderAndDetailVo orderAndDetailVo) {
        if (ObjectUtils.isEmpty(paramVo.getSource())) {
            return;
        }
        ProjectOrder order = orderAndDetailVo.getOrder();
        ProjectExpensePaymentSource source = paramVo.getSource();
        ProjectExpensePaymentSource updateSource = new ProjectExpensePaymentSource();
        updateSource.setId(source.getId());
        updateSource.setBizType(BizType.PROJECT_ORDER.getType());
        updateSource.setBizId(order.getId());
        updateSource.setLastUpdatedTime(new Date());
        updateSource.setModifiedBy("admin");
        sourceRepository.updateById(updateSource);
    }


    private OrderRequest buildOmsOrderReceiveVo(ProjectOrderAndDetailVo orderAndDetailVo, ProjectOrderRequestVo request) {
        ProjectOrder order = orderAndDetailVo.getOrder();
        List<ProjectOrderDetail> orderDetailList = orderAndDetailVo.getOrderDetails();
        ProjectInfo projectInfo = projectInfoRepository.selectById(order.getProjectId());

        OrderRequest omsOrderReceiveVo = new OrderRequest();
        omsOrderReceiveVo.setSourceType(OrderSourceType.APP);
        omsOrderReceiveVo.setProjectCode(projectInfo.getContractCode());
        omsOrderReceiveVo.setCompanyCode(projectInfo.getCompanyCode());

        List<OrderAttachs> attachs = Lists.newArrayList();
        orderDetailList.forEach(item->{
            if(attachs.size() == 0){
                OrderAttachs att = new OrderAttachs();
                att.setAttachUrl(item.getAttachmentUrl());
                attachs.add(att);
            }

        });
        omsOrderReceiveVo.setAttachs(attachs);
        omsOrderReceiveVo.setCreator(order.getCreatedBy());
        omsOrderReceiveVo.setIsEmergency(order.getEmergencyFlag());
        omsOrderReceiveVo.setPalcePerson(projectInfo.getProjectManagerCode());
        omsOrderReceiveVo.setCreator(projectInfo.getProjectManagerCode());
        omsOrderReceiveVo.setProjectManager(projectInfo.getProjectManagerCode());
        omsOrderReceiveVo.setCustomer(projectInfo.getCustomerName());
        omsOrderReceiveVo.setPmTelephone(projectInfo.getProjectManagerPhone());
        omsOrderReceiveVo.setProjectManager(projectInfo.getProjectManagerName());
        if(projectInfo.getType().equals(ProjectType.SOFT)){
            BaseBizOpDetailResponseVoForPm responseVo= bizOpFeign.findDetailInfoByIdForPm(projectInfo.getTrackId(),true);
            omsOrderReceiveVo.setPalcePerson(responseVo.getDesignerDisplay());
            omsOrderReceiveVo.setLinkman(responseVo.getDesignerDisplay());
            omsOrderReceiveVo.setLinkTelephone(responseVo.getDesignerPhone());
            omsOrderReceiveVo.setCreator(responseVo.getDesigner());
        }else{
            omsOrderReceiveVo.setPalcePerson(projectInfo.getProjectManagerName());
            omsOrderReceiveVo.setLinkman(projectInfo.getProjectManagerName());
            omsOrderReceiveVo.setLinkTelephone(projectInfo.getProjectManagerPhone());
        }

        omsOrderReceiveVo.setAddress(projectInfo.getProjectName());
        omsOrderReceiveVo.setRemark(order.getRemark());
        omsOrderReceiveVo.setType(OrderType.PURCHASE);
        try {
            omsOrderReceiveVo.setRequiredArriveDate(DateUtil.str2Date(request.getExpectUseDate()));
        } catch (ParseException e) {
            throw new SystemException("预计到达日期日期格式有误");
        }
        omsOrderReceiveVo.setSourceCode(order.getOrderCode());
        buildOrderAddressInfo(projectInfo, omsOrderReceiveVo);
        List<OrderDetailRequest> detailVos = Lists.newArrayList();
        buildOrderDetailRequest(orderDetailList, detailVos,projectInfo);
        omsOrderReceiveVo.setVendorCode(orderDetailList.get(0).getVendorCode());
        omsOrderReceiveVo.setDetails(detailVos);
        return omsOrderReceiveVo;
    }


    @Override
    public void buildOrderDetailRequest(List<ProjectOrderDetail> orderDetailList, List<OrderDetailRequest> detailVos, ProjectInfo projectInfo) {
        //BidContractCommonVo commonVo = bizopContractFeign.selectContractDetail(projectInfo.getTrackId());
        if(projectInfo.getType().equals(ProjectType.SOFT)) {
            List<String> skuCodeList = Lists.newArrayList();
//            BidPmsPriceRequestParam bidPmsPriceRequestParam = new BidPmsPriceRequestParam();
            List<BidPmsSkuCodeVo> skuCodeVos = Lists.newArrayList();
            orderDetailList.forEach(item -> {
                BidPmsSkuCodeVo skuCodeVo = new BidPmsSkuCodeVo();
                skuCodeVo.setSkuCode(item.getMaterialCode());
                skuCodeVos.add(skuCodeVo);
                skuCodeList.add(item.getMaterialCode());
            });
//            StoreLocation location = findStoreLocation(projectInfo.getCompanyCode());
//            Map<String, PurchasePrice> skuPriceMap = buildSkuPriceMap(skuCodeList, location.getId());
//
//            bidPmsPriceRequestParam.setPriceType("customerPrice");
//            bidPmsPriceRequestParam.setSkuCodes(skuCodeVos);
//            bidPmsPriceRequestParam.setBizopId(projectInfo.getTrackId());
//            Map<String, BidPmsPriceVo> priceVoMap  = bidPmsFeign.getPrice(bidPmsPriceRequestParam);
            //合同总金额
            //BigDecimal allAmount = commonVo.getTotalAmount();
            for (ProjectOrderDetail detail : orderDetailList) {
                OrderDetailRequest detailVo = new OrderDetailRequest();
                detailVo.setInstallPosition("");
//                BigDecimal customerPrice = priceVoMap.get(detail.getMaterialCode()).getPrice();
//                if(customerPrice == null || customerPrice.compareTo(BigDecimal.ZERO)<=0){
//                    //查询最新采购价
//                    PurchasePrice skuPurchasePrice = skuPriceMap.get(detail.getMaterialCode());
//                    if(ObjectUtils.isEmpty(skuPurchasePrice)){
//                        throw new BusinessException("未查到对应的采购价[" + detail.getMaterialCode() + "]");
//                    }
//                    if(ObjectUtils.isEmpty(skuPurchasePrice.getPrice())){
//                        throw new BusinessException("未查到对应的采购价[" + detail.getMaterialCode() + "]");
//                    }
//                    customerPrice = skuPurchasePrice.getPrice();
//                }

                //销售单位数量
                SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(detail.getMaterialCode());
                if (skuRelationDataVo != null) {
                    //采购单位 //销售单位
                    List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                    //获取采购单位和销售单位根据类型
                    if (!ObjectUtils.isEmpty(spuUnitList)) {
                        for (SpuUnit unit : spuUnitList) {
                            if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                                detailVo.setUnitCode(unit.getUnitCode());

                            }
//                            if (unit.getType().equals(SPUConstant.SALEUNIT)) {
//                                BigDecimal convertRate = unit.getConvertRate();
//                                if(convertRate.compareTo(BigDecimal.ZERO) ==0){
//                                    throw new BusinessException("物料单位转换率不能为0[" + detail.getMaterialCode()+"]");
//                                }
//                                customerPrice = customerPrice.divide(convertRate).setScale(5,BigDecimal.ROUND_HALF_UP);
//                            }
                        }
                    }
                }
/*                //计算客户金额: 采购金额*采购量
                BigDecimal customerAmount = customerPrice.multiply(detail.getOrderConvertNum());
                //计算比例：客户金额/总合同金额
                BigDecimal rate = customerAmount.divide(allAmount,5).setScale(5,BigDecimal.ROUND_HALF_UP);
                //计算分摊的销售价格 ：采购金额*分配比例
                detailVo.setSalePrice(customerPrice.multiply(rate).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());*/
                if (projectInfo.getType().equals(ProjectType.SOFT)) {
                    detailVo.setMaterialType(MaterialType.SOFT_SERVICE);
                } else {
                    detailVo.setMaterialType(MaterialType.MATERIAL);
                }
                detailVo.setQuantity(detail.getOrderConvertNum().doubleValue());
                detailVo.setRemark(detail.getRemark() + " " + detail.getDesignerRemark());
                detailVo.setSize(detail.getMeasureSize());
                detailVo.setSkuCode(detail.getMaterialCode());
                detailVo.setVendorCode(detail.getVendorCode());
                detailVo.setSourceLineNo(detail.getItemId());
                detailVo.setSalePrice(detail.getSalePrice()==null?0d:detail.getSalePrice().doubleValue());
                detailVos.add(detailVo);
            }
        }else {
            for (ProjectOrderDetail detail : orderDetailList) {
                OrderDetailRequest detailVo = new OrderDetailRequest();
                detailVo.setInstallPosition(detail.getPosition());
                if (projectInfo.getType().equals(ProjectType.SOFT)) {
                    detailVo.setMaterialType(MaterialType.SOFT_SERVICE);
                } else {
                    detailVo.setMaterialType(MaterialType.MATERIAL);
                }

                detailVo.setQuantity(detail.getOrderConvertNum().doubleValue());
                detailVo.setRemark(detail.getRemark() + " " + detail.getDesignerRemark());
                detailVo.setSize(detail.getMeasureSize());
                detailVo.setSkuCode(detail.getMaterialCode());
                //销售单位数量
                SkuRelationDataVo skuRelationDataVo = skuFeign.selectSkuRelationDataBySkuCode(detail.getMaterialCode());
                if (skuRelationDataVo != null) {
                    //采购单位 //销售单位
                    List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                    //获取采购单位和销售单位根据类型
                    if (!ObjectUtils.isEmpty(spuUnitList)) {
                        for (SpuUnit unit : spuUnitList) {
                            if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                                detailVo.setUnitCode(unit.getUnitCode());
                                break;
                            }
                        }
                    }
                }
                detailVo.setVendorCode(detail.getVendorCode());
                detailVo.setSourceLineNo(detail.getItemId());
                detailVos.add(detailVo);
            }
        }
    }

    private void buildOrderAddressInfo(ProjectInfo projectInfo, OrderRequest omsOrderReceiveVo) {
        BaseBizOp baseBizOp = baseBizOpFeign.findById(projectInfo.getTrackId());
        HouseInfo houseInfo = houseInfoServiceClient.selectById(baseBizOp.getHouseId());
        BuildingInfo buildingInfo = buildingInfoFeign.findOneById(houseInfo.getBuildingId());
        //BuildingInfo buildingInfo = bizOpFeign.findBuildingInfoById(projectInfo.getTrackId());
        if (!ObjectUtils.isEmpty(buildingInfo)) {
            Region mapRegion = new Region();
            if (!ObjectUtils.isEmpty(buildingInfo.getProvinceCode())) {
                mapRegion.setRegionCode(buildingInfo.getProvinceCode());
                //省
                Region regionProvince = regionFeign.findOneByEntity(mapRegion);
                if (!ObjectUtils.isEmpty(regionProvince)) {
                    omsOrderReceiveVo.setProvince(regionProvince.getRegionName());
                }
            }

            if (!ObjectUtils.isEmpty(buildingInfo.getCityCode())) {
                mapRegion.setRegionCode(buildingInfo.getProvinceCode());
                //市
                Region regionProvince = regionFeign.findOneByEntity(mapRegion);
                if (!ObjectUtils.isEmpty(regionProvince)) {
                    omsOrderReceiveVo.setCity(regionProvince.getRegionName());
                }
            }

            if (!ObjectUtils.isEmpty(buildingInfo.getCountyCode())) {
                //区
                mapRegion.setRegionCode(buildingInfo.getCountyCode());
                Region regionProvince = regionFeign.findOneByEntity(mapRegion);
                if (!ObjectUtils.isEmpty(regionProvince)) {
                    omsOrderReceiveVo.setCounty(regionProvince.getRegionName());
                }
            }

            /*String provice = buildingInfo.getProvinceName();
            String city = buildingInfo.getCityName();
            String areaName = buildingInfo.getBuildingAreaName();
            omsOrderReceiveVo.setProvince(provice);
            omsOrderReceiveVo.setCity(city);
            omsOrderReceiveVo.setCounty(areaName);*/
        }
    }

    private void updateProjectPlanTask(ProjectOrderRequestVo request,ProjectPlanTask projectPlanTask) {
        //ProjectPlanTask projectPlanTask = planTaskRepository.selectOneByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());
        if (ObjectUtils.isEmpty(projectPlanTask)) {
            return;
        }
        projectPlanTask.setActualStartDate(new Date());
        projectPlanTask.setActualStartDate(new Date());
        projectPlanTask.setStatus(ProjectTaskStatus.TStarted.getCode());
        projectPlanTask.fillOperationInfo(request.getEmployeeCode());
        planTaskRepository.updateById(projectPlanTask);
    }

    private void updateProjectPlanTask(ProjectOrderRequestVo request,List<ProjectPlanTask> projectPlanTaskList) {
        //ProjectPlanTask projectPlanTask = planTaskRepository.selectOneByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());
        if (ObjectUtils.isEmpty(projectPlanTaskList)) {
            return;
        }
        for(ProjectPlanTask projectPlanTask : projectPlanTaskList){
            projectPlanTask.setActualStartDate(new Date());
            projectPlanTask.setStatus(ProjectTaskStatus.TCompleted.getCode());
            projectPlanTask.fillOperationInfo(request.getEmployeeCode());
            planTaskRepository.updateById(projectPlanTask);
        }

    }

    private void updateMaterialType(ProjectOrderRequestVo request) {
        ProjectMaterialType materialType = materialTypeRepository.selectOneByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode());
        if(ObjectUtils.isEmpty(materialType)){
            return;
        }
        materialType.setMaterialType(materialType.getMaterialType());
        materialType.setProjectId(request.getProjectId());
        materialType.setStatus(PmsMaterialTypeStatus.MT_ORDERED.getCode());
        materialType.fillOperationInfo(request.getEmployeeCode());
        materialTypeRepository.updateById(materialType);
    }

    private List<ProjectOrderDetail> insertOrderAndDetails(ProjectOrderRequestVo request,
                                                           List<Long> newMaterialList,
                                                           ProjectOrderAndDetailVo orderAndDetailVo,
                                                           Map<Long, ProjectMaterial> materialMap,
                                                           ProjectPlanTask planTask,
                                                           ProjectInfo projectInfo) {

        //1.插入申请表数据
        String orderCode = codeRuleFeign.generateCode(BIZ_KEY, projectInfo.getCompanyCode().substring(0, 3));
        ProjectOrder order = new ProjectOrder();
        order.setProjectId(request.getProjectId());
        order.setOrderCode(orderCode);
        order.setEmergencyFlag(request.getEmergencyFlag()==1?true:false);
        order.setExpectUseDate(DateUtil.strToDate(request.getExpectUseDate()));
        order.setMaterialTypeCode(request.getMaterialTypeCode());
        order.setMaterialTypeName(request.getMaterialTypeName());
        //根据物料类别查询对应的任务
        order.setPlanTaskId(planTask == null ? 0 : planTask.getId());
        order.setRemark(request.getRemark());
        order.setStatus(PmsOrderStatus.PO_APPLYING.getCode());
        order.setOrderType(PmsOrderType.PO_NORMAL.getCode());
        order.fillOperationInfo(request.getEmployeeCode());
        orderRepository.insert(order);

        orderAndDetailVo.setOrder(order);

        LOGGER.info("######[ProjectOrderServiceImpl.insertOrderAndDetails]开始创建订单，订单单号：[" + orderCode + "]######");

        String url = null;
        if(CabinetType.CABINET.equals(request.getMaterialTypeCode())){
            List<ElecContractAttach> contractAttachList = elecContractAttachFeign.findInfo(projectInfo.getTrackId(),BidElectronicContractType.CABINET_CONTRACT.getCode());
            if(ObjectUtils.isEmpty(contractAttachList)){
                throw new BusinessException("橱柜图纸不存在，不能下单!合同号：[" + projectInfo.getContractCode() + "]");
            }
            url = contractAttachList.get(0).getFileSite();
        }else if(CabinetType.WARDROBE.equals(request.getMaterialTypeCode())){
            List<ElecContractAttach> contractAttachList = elecContractAttachFeign.findInfo(projectInfo.getTrackId(),BidElectronicContractType.WARDROBE_CONTRACT.getCode());
            if(ObjectUtils.isEmpty(contractAttachList)){
                throw new BusinessException("衣柜图纸不存在，不能下单!合同号：[" + projectInfo.getContractCode() + "]");
            }
            url = contractAttachList.get(0).getFileSite();
        }

        List<BidPmsBudgetDtlVo> pmsBudgetDtlVos = bidPmsFeign.selectAllContractMater(projectInfo.getTrackId());
        if (ObjectUtils.isEmpty(pmsBudgetDtlVos)) {
            throw new BusinessException("获取材料确认表数据为空！");
        }

        Set<String> skuCodeSet = request.getOrderDetailList().stream().map(ProjectOrderDetailResquest::getMaterialCode).collect(Collectors.toSet());

        List<BidPmsSkuCodeVo> skuCodeVos = Lists.newArrayList();
        List<String> skuCodeList = Lists.newArrayList();
        pmsBudgetDtlVos.forEach(item -> {
            BidPmsSkuCodeVo skuCodeVo = new BidPmsSkuCodeVo();
            skuCodeVo.setSkuCode(item.getSkuCode());
            skuCodeVos.add(skuCodeVo);
            skuCodeList.add(item.getSkuCode());
            skuCodeSet.add(item.getSkuCode());
        });


        Map<String,SkuRelationDataVo> skuRelationDataVoMap = materialInfoSearchService.getSkuDataInfo(skuCodeSet,null);
        List<Long> materialIds = request.getOrderDetailList().stream().filter(x->x.getProjectMaterialId()!=null).map(ProjectOrderDetailResquest::getProjectMaterialId).collect(Collectors.toList());
        List<ProjectMaterial> materialList = materialRepository.selectBatchIds(materialIds);
        Map<Long,ProjectMaterial> projectMaterialMap = materialList.stream().collect(Collectors.toMap(ProjectMaterial::getId, a -> a,(k1,k2)->k1));


//        BidContractCommonVo commonVo = bizopContractFeign.selectContractDetail(projectInfo.getTrackId());
//        BidPmsPriceRequestParam bidPmsPriceRequestParam = new BidPmsPriceRequestParam();
//        bidPmsPriceRequestParam.setPriceType("customerPrice");
//        bidPmsPriceRequestParam.setSkuCodes(skuCodeVos);
//        bidPmsPriceRequestParam.setBizopId(projectInfo.getTrackId());
//        Map<String, BidPmsPriceVo> priceVoMap  = bidPmsFeign.getPrice(bidPmsPriceRequestParam);
//        //合同总金额
//        BigDecimal allAmount = commonVo.getTotalAmount();
//
//        //预算总金额
//        BigDecimal customerAllAmount = BigDecimal.ZERO;
//        for (BidPmsBudgetDtlVo delVo : pmsBudgetDtlVos) {
//            SkuRelationDataVo skuRelationDataVo = skuRelationDataVoMap.get(delVo.getSkuCode());
//            //过滤计划量0或者负数的
//            if (BigDecimal.ZERO.compareTo(delVo.getPlanNumber()) >= 0) {
//                continue;
//            }
//            if (!skuRelationDataVo.getSpu().getType().equals(SpuMaterialTypeConstants.FMATERIAL.getCode())) {
//                continue;
//            }
//            BigDecimal customerPrice = BigDecimal.ZERO;
//            BidPmsPriceVo priceVo = priceVoMap.get(delVo.getSkuCode());
//            if(UtilTool.isNull(priceVo)){
//                customerPrice = BigDecimal.ZERO;
//            }else {
//                customerPrice = priceVo.getPrice();
//                if (customerPrice == null || customerPrice.compareTo(BigDecimal.ZERO) <= 0) {
//                    customerPrice = BigDecimal.ZERO;
//                }
//            }
//            customerAllAmount = customerAllAmount.add(delVo.getPlanNumber().multiply(customerPrice));
//        }

        //2.插入明细表数据
        List<ProjectOrderDetail> orderDetailList = Lists.newArrayList();
        for (ProjectOrderDetailResquest detailResquest : request.getOrderDetailList()) {
            ProjectMaterial material = projectMaterialMap.get(detailResquest.getProjectMaterialId());
            List<ProjectOrderDetail> projectOrderDetails = orderRepository.selectListByProjectMaterialId(detailResquest.getProjectMaterialId());
            if(!ObjectUtils.isEmpty(projectOrderDetails)){
                ProjectMaterial copy = new ProjectMaterial();
                BeanUtils.copy(material, copy);
                copy.setId(null);
                copy.setAddFlag(true);
                copy.setPickingNum(BigDecimal.ZERO);
                copy.setPickingConvertNum(BigDecimal.ZERO);
                copy.setOrderConvertNum(new BigDecimal(detailResquest.getOrderConvertNum()));
                copy.setOrderNum(new BigDecimal(detailResquest.getOrderNum()));
                copy.fillOperationInfo(request.getEmployeeCode());
                materialRepository.insert(copy);

                //插入状态记录表
                ProjectMaterialTrace materialTrace = new ProjectMaterialTrace();
                materialTrace.setFollowUserCode(request.getEmployeeCode());
                materialTrace.setProjectId(material.getProjectId());
                materialTrace.setProjectMaterialId(material.getId());
                materialTrace.setFollowTime(new Date());
                materialTrace.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
                materialTrace.setFollowUserName(request.getEmployeeCode());
                materialTrace.fillOperationInfo(request.getEmployeeCode());
                traceRepository.insert(materialTrace);

                detailResquest.setProjectMaterialId(copy.getId());
                newMaterialList.add(copy.getId());
            }else{
                materialMap.put(material.getId(),material);
            }

            ProjectOrderDetail orderDetail = new ProjectOrderDetail();
            orderDetail.setMaterialCode(detailResquest.getMaterialCode());
            orderDetail.setMaterialName(material.getMaterialName());
            orderDetail.setMeasureSize(detailResquest.getMeasureSize());
            //采购单位数量
            orderDetail.setOrderConvertNum(new BigDecimal(detailResquest.getOrderConvertNum()));
            //销售单位数量
            SkuRelationDataVo skuRelationDataVo = skuRelationDataVoMap.get(detailResquest.getMaterialCode());//skuFeign.selectSkuRelationDataBySkuCode(detailResquest.getMaterialCode());
            if (skuRelationDataVo != null) {
                //采购单位 //销售单位
                List<SpuUnit> spuUnitList = skuRelationDataVo.getSpuUnitList();
                //获取采购单位和销售单位根据类型
                if (!ObjectUtils.isEmpty(spuUnitList)) {
                    for (SpuUnit unit : spuUnitList) {
                        if (unit.getType().equals(SPUConstant.SALEUNIT)) {
                            orderDetail.setSaleUnit(unit.getUnitName());
                        }
                        if (unit.getType().equals(SPUConstant.PURCHASEUNIT)) {
                            orderDetail.setConvertUnit(unit.getUnitName());
                        }
                    }
                }
            }
            orderDetail.setOrderNum(new BigDecimal(detailResquest.getOrderNum()));
            orderDetail.setProManagerPrice(material.getProManagerPrice());
            orderDetail.setProjectMaterialId(detailResquest.getProjectMaterialId());
            orderDetail.setOrderId(order.getId());
            orderDetail.setProjectId(request.getProjectId());
            if(ObjectUtils.isEmpty(detailResquest.getMeasureRemark())){
                orderDetail.setRemark(detailResquest.getRemark());
            }else {
                orderDetail.setRemark(detailResquest.getRemark() + ", 测量备注：[" + detailResquest.getMeasureRemark() + "]");
            }
            orderDetail.setDesignerRemark(detailResquest.getDesigerRemark());
            orderDetail.setSpecialCode(material.getSpecialCode());
            orderDetail.setPosition(detailResquest.getSpace());
            //orderDetail.setConvertUnit(material.getConvertUnit());
            //orderDetail.setSaleUnit(material.getSaleUnit());
            orderDetail.setAttachmentUrl(url);
            orderDetail.setStatus(PmsMaterialStatus.M_ORDERED.getCode());
            orderDetail.setVendorCode(detailResquest.getVendorCode());
            orderDetail.fillOperationInfo(request.getEmployeeCode());

//            BigDecimal customerPrice = priceVoMap.get(orderDetail.getMaterialCode()).getPrice();
//            StoreLocation location = findStoreLocation(projectInfo.getCompanyCode());
//            Map<String, PurchasePrice> skuPriceMap = buildSkuPriceMap(skuCodeList, location.getId());
//            if(customerPrice == null || customerPrice.compareTo(BigDecimal.ZERO)<=0){
//                //查询最新采购价
//                PurchasePrice skuPurchasePrice = skuPriceMap.get(orderDetail.getMaterialCode());
//                if(ObjectUtils.isEmpty(skuPurchasePrice)){
//                    throw new BusinessException("未查到对应的采购价[" + orderDetail.getMaterialCode() + "]");
//                }
//                if(ObjectUtils.isEmpty(skuPurchasePrice.getPrice())){
//                    throw new BusinessException("未查到对应的采购价[" + orderDetail.getMaterialCode() + "]");
//                }
//                customerPrice = skuPurchasePrice.getPrice();
//            }
//            //计算比例：客户金额*合同价格/客户价格*数量综合
//            BigDecimal salePrice = customerPrice.multiply(allAmount).divide(customerAllAmount,5,BigDecimal.ROUND_DOWN);
            //BigDecimal rate = allAmount.divide(customerAllAmount,5,RoundingMode.DOWN);
            //计算分摊的销售价格 ：采购金额*分配比例
//            orderDetail.setSalePrice(salePrice);

            orderDetailList.add(orderDetail);
        }
        orderDetailRepository.insertBatch(orderDetailList);
        //更新订单行号规则id
        for (ProjectOrderDetail orderDetail : orderDetailList) {
            orderDetail.setItemId(orderDetail.getId().toString());
            orderDetailRepository.updateById(orderDetail);
        }

        orderAndDetailVo.setOrderDetails(orderDetailList);
        return orderDetailList;
    }

    /**
     * 获取订单详情的材料类型
     */
    private String getMaterialType(Long orderId){
        List<String> types=Lists.newArrayList();
        //根据订单id获取项目主材集合
        List<ProjectMaterial> materials=materialRepository.selectListByOrderId(orderId);
        for(ProjectMaterial material:materials){
            String type=material.getMaterialMiddleTypeName();
            if(types.contains(type)){
                continue;
            }
            types.add(type);
        }
        return types.size()>0?StringUtils.join(types).replace("[","").replace("]",""):"";
    }


    public OrderRequest buildOmsOrderRequestForSoftOrder(ProjectOrderRequestVo request) {
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单申请参数: [" + JSON.toJSONString(request) + "]######");


        final ProjectInfo info = projectInfoRepository.selectById(request.getProjectId());
        if(!info.getProjectStatus().equals(ProjectStatus.pUnderConstruction)){
            throw new BusinessException("项目必须是在建才可以操作!");
        }

        if(!ProjectType.SOFT.equals(info.getType())){
            CostData costData = new CostData();
            costData.setIsDeleted(0);
            costData.setBizOpId(info.getTrackId());
            costData.setAccountType(DictionaryConstants.ACCOUNT_TYPE_SEND_PACK);
            costData.setState(DictionaryConstants.COST_COMPLETE);
            List<CostData> costDataList = tamsCostFeign.list(costData);

            if (ObjectUtils.isEmpty(costDataList)) {
                LOGGER.error("######[ProjectOrderServiceImpl.addProjectOrder]发包查询失败：项目经理发包表为空【" + info.getContractCode()+"]######");
                throw new BusinessException("项目经理未确认接包！商机ID[" + info.getTrackId() +"]");
            }
        }

        ProjectPlanTask projectPlanTask = planTaskRepository.selectOneByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());

        List<Long> newMaterialList = Lists.newArrayList();
        ProjectOrderAndDetailVo orderAndDetailVo = new ProjectOrderAndDetailVo();
        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        //1.插入订单申请数据
        List<ProjectOrderDetail> orderDetailList = insertOrderAndDetails(request, newMaterialList, orderAndDetailVo,materialMap,projectPlanTask,info);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]保存订单明细，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        //2.更新计划任务状态
        updateProjectPlanTask(request,projectPlanTask);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单更新计划任务，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //3.更新项目主材表数据
        updateProjectMaterial(orderDetailList, request, newMaterialList,materialMap);
        LOGGER.info("######订单更新主材数据，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //4.更新物料类别状态为已下单
        updateMaterialType(request);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单更新物料类别状态，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //5.构建oms订单所需参数对象
        OrderRequest orderRequest = buildOmsOrderReceiveVo(orderAndDetailVo,request);

        //6.同步订单到OMS
        //LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统开始，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");
        //produce.sendOrderToOms(orderRequest);
        //LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统结束，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //String jsonStr = JSONObject.toJSONString(orderRequest);
        //LOGGER.info("[ProjectOrderServiceImpl.addProjectOrder]报文测试：[" + jsonStr+"]");

        return orderRequest;
    }



    public OrderRequest buildOmsOrderRequestForGfitOrder(ProjectOrderRequestVo request) {
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单申请参数: [" + JSON.toJSONString(request) + "]######");


        final ProjectInfo info = projectInfoRepository.selectById(request.getProjectId());
        if(!info.getProjectStatus().equals(ProjectStatus.pUnderConstruction)){
            //throw new BusinessException("项目必须是在建才可以操作!");
        }

        if(!ProjectType.SOFT.equals(info.getType())){
            CostData costData = new CostData();
            costData.setIsDeleted(0);
            costData.setBizOpId(info.getTrackId());
            costData.setAccountType(DictionaryConstants.ACCOUNT_TYPE_SEND_PACK);
            costData.setState(DictionaryConstants.COST_COMPLETE);
            List<CostData> costDataList = tamsCostFeign.list(costData);

            if (ObjectUtils.isEmpty(costDataList)) {
                LOGGER.error("######[ProjectOrderServiceImpl.addProjectOrder]发包查询失败：项目经理发包表为空【" + info.getContractCode()+"]######");
                throw new BusinessException("项目经理未确认接包！商机ID[" + info.getTrackId() +"]");
            }
        }

        ProjectPlanTask projectPlanTask = planTaskRepository.selectOneByTypeAndProjectId(request.getProjectId(), request.getMaterialTypeCode(), PmsTaskType.ORDER.getCode());

        List<Long> newMaterialList = Lists.newArrayList();
        ProjectOrderAndDetailVo orderAndDetailVo = new ProjectOrderAndDetailVo();
        Map<Long, ProjectMaterial> materialMap = new HashMap<>();
        //1.插入订单申请数据
        List<ProjectOrderDetail> orderDetailList = insertOrderAndDetails(request, newMaterialList, orderAndDetailVo,materialMap,projectPlanTask,info);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]保存订单明细，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");

        //2.更新计划任务状态
        updateProjectPlanTask(request,projectPlanTask);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单更新计划任务，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //3.更新项目主材表数据
        updateProjectMaterial(orderDetailList, request, newMaterialList,materialMap);
        LOGGER.info("######订单更新主材数据，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //4.更新物料类别状态为已下单
        updateMaterialType(request);
        LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单更新物料类别状态，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //5.构建oms订单所需参数对象
        OrderRequest orderRequest = buildOmsOrderReceiveVo(orderAndDetailVo,request);

        //6.同步订单到OMS
        //LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统开始，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");
        //produce.sendOrderToOms(orderRequest);
        //LOGGER.info("######[ProjectOrderServiceImpl.addProjectOrder]订单同步消息至OMS系统结束，单号[" + orderAndDetailVo.getOrder().getOrderCode() + "]######");


        //String jsonStr = JSONObject.toJSONString(orderRequest);
        //LOGGER.info("[ProjectOrderServiceImpl.addProjectOrder]报文测试：[" + jsonStr+"]");

        return orderRequest;
    }

}

