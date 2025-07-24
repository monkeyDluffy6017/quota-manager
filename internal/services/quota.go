package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"quota-manager/internal/config"
	"quota-manager/internal/database"
	"quota-manager/internal/models"
	"quota-manager/pkg/aigateway"
	"quota-manager/pkg/logger"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// QuotaService handles quota-related operations
type QuotaService struct {
	db              *database.DB
	aiGatewayConf   *config.AiGatewayConfig
	config          *config.Config
	aiGatewayClient interface {
		QueryGithubStarProjects(employeeNumber string) (*aigateway.StarProjectsResponse, error)
		SetGithubStarProjects(employeeNumber string, starredProjects string) error
	}
	voucherSvc *VoucherService
}

// NewQuotaService creates a new quota service
func NewQuotaService(db *database.DB, config *config.Config, aiGatewayClient interface {
	QueryGithubStarProjects(employeeNumber string) (*aigateway.StarProjectsResponse, error)
	SetGithubStarProjects(employeeNumber string, starredProjects string) error
}, voucherSvc *VoucherService) *QuotaService {
	return &QuotaService{
		db:              db,
		aiGatewayConf:   &config.AiGateway,
		config:          config,
		aiGatewayClient: aiGatewayClient,
		voucherSvc:      voucherSvc,
	}
}

// QuotaInfo represents user quota information
type QuotaInfo struct {
	TotalQuota float64           `json:"total_quota"`
	UsedQuota  float64           `json:"used_quota"`
	QuotaList  []QuotaDetailItem `json:"quota_list"`
}

// QuotaDetailItem represents quota detail item
type QuotaDetailItem struct {
	Amount     float64   `json:"amount"`
	ExpiryDate time.Time `json:"expiry_date"`
}

// QuotaAuditRecord represents quota audit record
type QuotaAuditRecord struct {
	Amount       float64                   `json:"amount"`
	Operation    string                    `json:"operation"`
	VoucherCode  string                    `json:"voucher_code,omitempty"`
	RelatedUser  string                    `json:"related_user,omitempty"`
	StrategyName string                    `json:"strategy_name,omitempty"`
	ExpiryDate   time.Time                 `json:"expiry_date"`
	Details      *models.QuotaAuditDetails `json:"details,omitempty"`
	CreateTime   time.Time                 `json:"create_time"`
}

// TransferOutRequest represents transfer out request
type TransferOutRequest struct {
	ReceiverID string              `json:"receiver_id" validate:"required,uuid"`
	QuotaList  []TransferQuotaItem `json:"quota_list" validate:"required,min=1,dive"`
}

// TransferQuotaItem represents quota item for transfer
type TransferQuotaItem struct {
	Amount     float64   `json:"amount" validate:"required,gt=0"`
	ExpiryDate time.Time `json:"expiry_date" validate:"required"`
}

// TransferOutResponse represents transfer out response
type TransferOutResponse struct {
	VoucherCode string              `json:"voucher_code"`
	RelatedUser string              `json:"related_user"`
	Operation   string              `json:"operation"`
	QuotaList   []TransferQuotaItem `json:"quota_list"`
}

// TransferInRequest represents transfer in request
type TransferInRequest struct {
	VoucherCode string `json:"voucher_code" validate:"required,min=10,max=2000"`
}

// TransferStatus represents the transfer status
type TransferStatus string

const (
	TransferStatusSuccess         TransferStatus = "SUCCESS"
	TransferStatusPartialSuccess  TransferStatus = "PARTIAL_SUCCESS"
	TransferStatusFailed          TransferStatus = "FAILED"
	TransferStatusAlreadyRedeemed TransferStatus = "ALREADY_REDEEMED"
)

// TransferFailureReason represents the reason for transfer failure
type TransferFailureReason string

const (
	TransferFailureReasonExpired TransferFailureReason = "EXPIRED"
	TransferFailureReasonPending TransferFailureReason = "PENDING"
)

// TransferInResponse represents transfer in response
type TransferInResponse struct {
	GiverID     string                `json:"giver_id"`
	GiverName   string                `json:"giver_name"`
	GiverPhone  string                `json:"giver_phone"`
	GiverGithub string                `json:"giver_github"`
	ReceiverID  string                `json:"receiver_id"`
	QuotaList   []TransferQuotaResult `json:"quota_list"`
	VoucherCode string                `json:"voucher_code"`
	Operation   string                `json:"operation"`
	Amount      float64               `json:"amount"`
	Status      TransferStatus        `json:"status"`
	Message     string                `json:"message,omitempty"`
}

// TransferQuotaResult represents transfer quota result
type TransferQuotaResult struct {
	Amount        float64                `json:"amount"`
	ExpiryDate    time.Time              `json:"expiry_date"`
	IsExpired     bool                   `json:"is_expired"`
	Success       bool                   `json:"success"`
	FailureReason *TransferFailureReason `json:"failure_reason,omitempty"`
}

// GetUserQuota retrieves user quota information
func (s *QuotaService) GetUserQuota(userID string) (*QuotaInfo, error) {
	// Get total quota from AiGateway
	totalQuota, err := s.getQuotaFromAiGateway(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get total quota: %w", err)
	}

	// Get used quota from AiGateway
	usedQuota, err := s.getUsedQuotaFromAiGateway(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get used quota: %w", err)
	}

	// Get quota list from database
	var quotas []models.Quota
	if err := s.db.DB.Where("user_id = ? AND status = ?", userID, models.StatusValid).
		Order("expiry_date ASC").Find(&quotas).Error; err != nil {
		return nil, fmt.Errorf("failed to get quota list: %w", err)
	}

	// Calculate remaining quotas considering used quota
	quotaList := make([]QuotaDetailItem, 0)
	remainingUsed := usedQuota

	for _, quota := range quotas {
		if remainingUsed <= 0 {
			// No more used quota to deduct
			quotaList = append(quotaList, QuotaDetailItem{
				Amount:     quota.Amount,
				ExpiryDate: quota.ExpiryDate,
			})
		} else if quota.Amount > remainingUsed {
			// This quota is partially consumed
			quotaList = append(quotaList, QuotaDetailItem{
				Amount:     quota.Amount - remainingUsed,
				ExpiryDate: quota.ExpiryDate,
			})
			remainingUsed = 0
		} else {
			// This quota is fully consumed
			remainingUsed -= quota.Amount
		}
	}

	return &QuotaInfo{
		TotalQuota: totalQuota,
		UsedQuota:  usedQuota,
		QuotaList:  quotaList,
	}, nil
}

// GetQuotaAuditRecords retrieves quota audit records
func (s *QuotaService) GetQuotaAuditRecords(userID string, page, pageSize int) ([]QuotaAuditRecord, int64, error) {
	var records []models.QuotaAudit
	var total int64

	offset := (page - 1) * pageSize

	// Get total count
	if err := s.db.DB.Model(&models.QuotaAudit{}).Where("user_id = ?", userID).Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count audit records: %w", err)
	}

	// Get records with pagination
	if err := s.db.DB.Where("user_id = ?", userID).
		Order("create_time DESC, id DESC").
		Offset(offset).
		Limit(pageSize).
		Find(&records).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to get audit records: %w", err)
	}

	result := make([]QuotaAuditRecord, len(records))
	for i, record := range records {
		// Parse details if available
		var details *models.QuotaAuditDetails
		if record.Details != "" {
			parsedDetails, err := record.UnmarshalDetails()
			if err == nil {
				details = parsedDetails
			}
		}

		result[i] = QuotaAuditRecord{
			Amount:       record.Amount,
			Operation:    record.Operation,
			VoucherCode:  record.VoucherCode,
			RelatedUser:  record.RelatedUser,
			StrategyName: record.StrategyName,
			ExpiryDate:   record.ExpiryDate,
			Details:      details,
			CreateTime:   record.CreateTime,
		}
	}

	return result, total, nil
}

// TransferOut handles quota transfer out
func (s *QuotaService) TransferOut(giver *models.AuthUser, req *TransferOutRequest) (*TransferOutResponse, error) {
	// Check if receiver_id is empty
	if req.ReceiverID == "" {
		return nil, NewValidationFailedError("receiver_id cannot be empty")
	}

	// Get used quota from AiGateway to check availability
	usedQuota, err := s.getUsedQuotaFromAiGateway(giver.ID)
	if err != nil {
		return nil, NewDatabaseError("get used quota", err)
	}

	// Get quota list ordered by expiry date to check availability
	var quotas []models.Quota
	if err := s.db.DB.Where("user_id = ? AND status = ?", giver.ID, models.StatusValid).
		Order("expiry_date ASC").Find(&quotas).Error; err != nil {
		return nil, fmt.Errorf("failed to get quota list: %w", err)
	}

	// Calculate remaining quotas for each expiry date
	quotaAvailabilityMap := make(map[string]float64) // key: expiry_date as string, value: available amount
	remainingUsed := usedQuota

	for _, quota := range quotas {
		dateKey := quota.ExpiryDate.Format("2006-01-02T15:04:05Z07:00")
		var availableFromThisQuota float64
		if remainingUsed <= 0 {
			availableFromThisQuota = quota.Amount
		} else if quota.Amount > remainingUsed {
			availableFromThisQuota = quota.Amount - remainingUsed
			remainingUsed = 0
		} else {
			availableFromThisQuota = 0
			remainingUsed -= quota.Amount
		}

		// Add to existing amount for the same expiry date (accumulate instead of overwriting)
		quotaAvailabilityMap[dateKey] += availableFromThisQuota
	}

	// Start transaction
	tx := s.db.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Debug: Print config info
	fmt.Printf("DEBUG: GitHub star check config - Enabled: %v, RequiredRepo: %s\n",
		s.config.GithubStarCheck.Enabled, s.config.GithubStarCheck.RequiredRepo)

	// Get giver's starred projects from database
	var giverGithubStar string
	var userInfo models.UserInfo
	if err := s.db.AuthDB.Where("id = ?", giver.ID).First(&userInfo).Error; err == nil {
		// Store all starred projects as comma-separated string
		giverGithubStar = userInfo.GithubStar
		// Debug: Print user info from database
		fmt.Printf("DEBUG: User info from database - ID: %s, GithubStar: %s\n", userInfo.ID, userInfo.GithubStar)
	}

	// checkGithubStar checks if user has starred the required GitHub repository
	if s.config.GithubStarCheck.Enabled {
		// Debug: Print star check info
		fmt.Printf("DEBUG: GitHub star check enabled, required repo: %s, user starred projects: %s\n",
			s.config.GithubStarCheck.RequiredRepo, giverGithubStar)

		isStar := false
		// Parse comma-separated starred projects
		starredProjects := strings.Split(giverGithubStar, ",")

		// Debug: Print parsed projects
		fmt.Printf("DEBUG: Parsed starred projects: %v\n", starredProjects)

		// Check if required repo is starred
		requiredRepo := strings.TrimSpace(s.config.GithubStarCheck.RequiredRepo)
		for _, project := range starredProjects {
			project = strings.TrimSpace(project)
			if project == requiredRepo {
				isStar = true
				fmt.Printf("DEBUG: Found required repo %s in user's starred projects\n", requiredRepo)
			}
		}

		if isStar == false {
			fmt.Printf("DEBUG: User has not starred required repo %s, returning error\n", requiredRepo)
			return nil, NewGithubStarRequiredError(requiredRepo)
		}
		fmt.Printf("DEBUG: User has starred required repo %s, allowing transfer\n", requiredRepo)
	}

	// Validate quota availability for each requested quota
	for _, quotaItem := range req.QuotaList {
		dateKey := quotaItem.ExpiryDate.Format("2006-01-02T15:04:05Z07:00")
		available, exists := quotaAvailabilityMap[dateKey]
		if !exists {
			tx.Rollback()
			return nil, fmt.Errorf("quota not found for expiry date %v", quotaItem.ExpiryDate)
		}

		if available < quotaItem.Amount {
			tx.Rollback()
			return nil, fmt.Errorf("insufficient available quota for expiry date %v: have %g, need %g",
				quotaItem.ExpiryDate, available, quotaItem.Amount)
		}

		// Also validate the total quota exists in database for this expiry date
		var totalQuotaAmount float64
		// Log the query parameters for debugging
		logger.Info("Checking quota availability",
			zap.String("user_id", giver.ID),
			zap.Time("expiry_date", quotaItem.ExpiryDate),
			zap.Float64("requested_amount", quotaItem.Amount),
			zap.String("status", models.StatusValid))

		if err := tx.Model(&models.Quota{}).
			Where("user_id = ? AND expiry_date = ? AND status = ?",
				giver.ID, quotaItem.ExpiryDate, models.StatusValid).
			Select("COALESCE(SUM(amount), 0)").
			Scan(&totalQuotaAmount).Error; err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to check quota for expiry date %v: %w", quotaItem.ExpiryDate, err)
		}

		// Log the result of the query
		logger.Info("Quota availability check result",
			zap.Float64("total_quota_amount", totalQuotaAmount),
			zap.Float64("requested_amount", quotaItem.Amount),
			zap.Bool("sufficient_quota", totalQuotaAmount >= quotaItem.Amount))

		if totalQuotaAmount < quotaItem.Amount {
			tx.Rollback()
			return nil, fmt.Errorf("insufficient quota for expiry date %v: have %f, need %f",
				quotaItem.ExpiryDate, totalQuotaAmount, quotaItem.Amount)
		}
	}

	// Generate voucher code
	voucherQuotaList := make([]VoucherQuotaItem, len(req.QuotaList))
	for i, item := range req.QuotaList {
		voucherQuotaList[i] = VoucherQuotaItem{
			Amount:     item.Amount,
			ExpiryDate: item.ExpiryDate,
		}
	}

	// Clean receiver_id to remove leading/trailing whitespace before generating voucher
	cleanReceiverID := strings.TrimSpace(req.ReceiverID)

	voucherData := &VoucherData{
		GiverID:         giver.ID,
		GiverName:       giver.Name,
		GiverPhone:      giver.Phone,
		GiverGithub:     giver.Github,
		GiverGithubStar: giverGithubStar, // Now stores comma-separated list of starred projects
		ReceiverID:      cleanReceiverID,
		QuotaList:       voucherQuotaList,
	}

	voucherCode, err := s.voucherSvc.GenerateVoucher(voucherData)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to generate voucher: %w", err)
	}

	// Update quota table - reduce giver's quota
	for _, quotaItem := range req.QuotaList {
		if err := tx.Model(&models.Quota{}).
			Where("user_id = ? AND expiry_date = ? AND status = ?",
				giver.ID, quotaItem.ExpiryDate, models.StatusValid).
			Update("amount", gorm.Expr("amount - ?", quotaItem.Amount)).Error; err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to update quota: %w", err)
		}

		// Delete quota records with zero or negative amounts
		if err := tx.Where("user_id = ? AND expiry_date = ? AND status = ? AND amount <= 0",
			giver.ID, quotaItem.ExpiryDate, models.StatusValid).Delete(&models.Quota{}).Error; err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to delete zero quota records: %w", err)
		}
	}

	// Calculate total amount for audit record
	totalAmount := 0.0
	// Find earliest expiry date for audit record
	var earliestExpiryDate time.Time
	for i, item := range req.QuotaList {
		totalAmount += item.Amount
		if i == 0 || item.ExpiryDate.Before(earliestExpiryDate) {
			earliestExpiryDate = item.ExpiryDate
		}
	}

	// Prepare detailed audit information
	auditDetails := &models.QuotaAuditDetails{
		Operation: models.OperationTransferOut,
		Summary: models.QuotaAuditSummary{
			TotalAmount:        totalAmount,
			TotalItems:         len(req.QuotaList),
			SuccessfulItems:    len(req.QuotaList), // All items are successful in transfer out
			EarliestExpiryDate: earliestExpiryDate.Format(time.RFC3339),
		},
		Items: make([]models.QuotaAuditDetailItem, len(req.QuotaList)),
	}

	// Record each quota item detail
	for i, item := range req.QuotaList {
		auditDetails.Items[i] = models.QuotaAuditDetailItem{
			Amount:     item.Amount,
			ExpiryDate: item.ExpiryDate.Format(time.RFC3339),
			Status:     models.AuditStatusSuccess,
		}
	}

	// Record audit log
	auditRecord := &models.QuotaAudit{
		UserID:      giver.ID,
		Amount:      -totalAmount,
		Operation:   models.OperationTransferOut,
		VoucherCode: voucherCode,
		RelatedUser: cleanReceiverID,
		ExpiryDate:  earliestExpiryDate, // Use earliest expiry date for audit
		// StrategyName is empty for transfer operations
	}
	if err := auditRecord.MarshalDetails(auditDetails); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to marshal audit details: %w", err)
	}
	if err := tx.Create(auditRecord).Error; err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to create audit record: %w", err)
	}

	// Update AiGateway quota
	if err := s.deltaQuotaInAiGateway(giver.ID, -totalAmount); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to update AiGateway quota: %w", err)
	}

	tx.Commit()

	return &TransferOutResponse{
		VoucherCode: voucherCode,
		RelatedUser: cleanReceiverID,
		Operation:   models.OperationTransferOut,
		QuotaList:   req.QuotaList,
	}, nil
}

// TransferIn handles quota transfer in
func (s *QuotaService) TransferIn(receiver *models.AuthUser, req *TransferInRequest) (*TransferInResponse, error) {
	// Validate voucher
	voucherData, err := s.voucherSvc.ValidateAndDecodeVoucher(req.VoucherCode)
	if err != nil {
		return &TransferInResponse{
			Status:  TransferStatusFailed,
			Message: "Invalid voucher code",
		}, nil
	}

	// Check if voucher is for the correct receiver
	if voucherData.ReceiverID != receiver.ID {
		return &TransferInResponse{
			Status:  TransferStatusFailed,
			Message: "Voucher is not for this user",
		}, nil
	}

	// Check if voucher has already been redeemed
	var existingRedemption models.VoucherRedemption
	if err := s.db.DB.Where("voucher_code = ?", req.VoucherCode).First(&existingRedemption).Error; err == nil {
		return &TransferInResponse{
			GiverID:     voucherData.GiverID,
			GiverName:   voucherData.GiverName,
			GiverPhone:  voucherData.GiverPhone,
			GiverGithub: voucherData.GiverGithub,
			ReceiverID:  receiver.ID,
			VoucherCode: req.VoucherCode,
			Operation:   models.OperationTransferIn,
			Status:      TransferStatusAlreadyRedeemed,
			Message:     "Voucher has already been redeemed",
		}, nil
	}

	// Start transaction
	tx := s.db.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Record redemption to prevent duplicate usage
	redemption := &models.VoucherRedemption{
		VoucherCode: req.VoucherCode,
		ReceiverID:  receiver.ID,
	}
	if err := tx.Create(redemption).Error; err != nil {
		tx.Rollback()
		return &TransferInResponse{
			Status:  TransferStatusFailed,
			Message: "Failed to record voucher redemption",
		}, nil
	}

	totalAmount := 0.0
	successCount := 0
	quotaResults := make([]TransferQuotaResult, len(voucherData.QuotaList))
	var earliestExpiryDate time.Time
	hasValidQuota := false

	// Process quota transfer
	for i, quotaItem := range voucherData.QuotaList {
		isExpired := time.Now().Truncate(time.Second).After(quotaItem.ExpiryDate.Truncate(time.Second))

		quotaResult := TransferQuotaResult{
			Amount:     quotaItem.Amount,
			ExpiryDate: quotaItem.ExpiryDate,
			IsExpired:  isExpired,
			Success:    false,
		}

		// Only process valid quota
		if !isExpired {
			var existingQuota models.Quota
			if err := tx.Where("user_id = ? AND expiry_date = ? AND status = ?",
				receiver.ID, quotaItem.ExpiryDate, models.StatusValid).First(&existingQuota).Error; err != nil {
				// Create new quota record
				newQuota := &models.Quota{
					UserID:     receiver.ID,
					Amount:     quotaItem.Amount,
					ExpiryDate: quotaItem.ExpiryDate,
					Status:     models.StatusValid,
				}
				if err := tx.Create(newQuota).Error; err != nil {
					// Individual quota creation failed, mark as pending
					reason := TransferFailureReasonPending
					quotaResult.FailureReason = &reason
				} else {
					quotaResult.Success = true
					successCount++
					totalAmount += quotaItem.Amount

					// Track earliest expiry date for valid quota
					if !hasValidQuota || quotaItem.ExpiryDate.Before(earliestExpiryDate) {
						earliestExpiryDate = quotaItem.ExpiryDate
						hasValidQuota = true
					}
				}
			} else {
				// Update existing quota
				if err := tx.Model(&existingQuota).Update("amount", existingQuota.Amount+quotaItem.Amount).Error; err != nil {
					// Individual quota update failed, mark as pending
					reason := TransferFailureReasonPending
					quotaResult.FailureReason = &reason
				} else {
					quotaResult.Success = true
					successCount++
					totalAmount += quotaItem.Amount

					// Track earliest expiry date for valid quota
					if !hasValidQuota || quotaItem.ExpiryDate.Before(earliestExpiryDate) {
						earliestExpiryDate = quotaItem.ExpiryDate
						hasValidQuota = true
					}
				}
			}
		} else {
			// Mark expired quota
			reason := TransferFailureReasonExpired
			quotaResult.FailureReason = &reason
		}

		quotaResults[i] = quotaResult
	}

	// Record audit log for valid quota only if there's valid quota
	if hasValidQuota {
		// Prepare detailed audit information
		expiredCount := 0
		failedCount := 0
		auditDetails := &models.QuotaAuditDetails{
			Operation: models.OperationTransferIn,
			Items:     make([]models.QuotaAuditDetailItem, len(quotaResults)),
		}

		// Record each quota item detail
		for i, result := range quotaResults {
			item := models.QuotaAuditDetailItem{
				Amount:     result.Amount,
				ExpiryDate: result.ExpiryDate.Format(time.RFC3339),
			}

			if result.IsExpired {
				item.Status = models.AuditStatusExpired
				item.FailureReason = "Quota expired"
				expiredCount++
			} else if result.Success {
				item.Status = models.AuditStatusSuccess
			} else {
				item.Status = models.AuditStatusFailed
				if result.FailureReason != nil {
					item.FailureReason = string(*result.FailureReason)
				}
				failedCount++
			}

			auditDetails.Items[i] = item
		}

		auditDetails.Summary = models.QuotaAuditSummary{
			TotalAmount:        totalAmount,
			TotalItems:         len(voucherData.QuotaList),
			SuccessfulItems:    successCount,
			FailedItems:        failedCount,
			ExpiredItems:       expiredCount,
			EarliestExpiryDate: earliestExpiryDate.Format(time.RFC3339),
		}

		auditRecord := &models.QuotaAudit{
			UserID:      receiver.ID,
			Amount:      totalAmount,
			Operation:   models.OperationTransferIn,
			VoucherCode: req.VoucherCode,
			RelatedUser: voucherData.GiverID,
			ExpiryDate:  earliestExpiryDate, // Use earliest expiry date from valid quota
			// StrategyName is empty for transfer operations
		}
		if err := auditRecord.MarshalDetails(auditDetails); err != nil {
			tx.Rollback()
			return &TransferInResponse{
				Status:  TransferStatusFailed,
				Message: "Failed to marshal audit details",
			}, nil
		}
		if err := tx.Create(auditRecord).Error; err != nil {
			tx.Rollback()
			return &TransferInResponse{
				Status:  TransferStatusFailed,
				Message: "Failed to create audit record",
			}, nil
		}
	}

	// Update AiGateway quota only for valid quota
	if totalAmount > 0 {
		if err := s.deltaQuotaInAiGateway(receiver.ID, totalAmount); err != nil {
			tx.Rollback()
			return &TransferInResponse{
				Status:  TransferStatusFailed,
				Message: "Failed to update AiGateway quota",
			}, nil
		}
	}

	// Check and handle GitHub star status if giver has starred projects
	if voucherData.GiverGithubStar != "" && s.aiGatewayClient != nil {
		// If giver has starred projects, set starred projects in AiGateway for receiver
		// This is best effort - we don't want to fail the transfer if AI Gateway call fails
		if err := s.aiGatewayClient.SetGithubStarProjects(receiver.ID, voucherData.GiverGithubStar); err != nil {
			logger.Warn("Failed to set GitHub star projects in AiGateway",
				zap.String("user_id", receiver.ID),
				zap.String("starred_projects", voucherData.GiverGithubStar),
				zap.Error(err))
		}
	}

	tx.Commit()

	// Determine overall transfer status
	var status TransferStatus
	var message string
	totalQuotas := len(voucherData.QuotaList)
	expiredCount := 0
	for _, result := range quotaResults {
		if result.IsExpired {
			expiredCount++
		}
	}

	if successCount == 0 {
		status = TransferStatusFailed
		message = "All quota transfers failed"
	} else if successCount == totalQuotas {
		status = TransferStatusSuccess
		message = "All quota transfers completed successfully"
	} else if successCount > 0 && expiredCount > 0 {
		status = TransferStatusPartialSuccess
		message = fmt.Sprintf("%d of %d quota transfers completed successfully, %d expired", successCount, totalQuotas, expiredCount)
	} else {
		status = TransferStatusPartialSuccess
		message = fmt.Sprintf("%d of %d quota transfers completed successfully", successCount, totalQuotas)
	}

	return &TransferInResponse{
		GiverID:     voucherData.GiverID,
		GiverName:   voucherData.GiverName,
		GiverPhone:  voucherData.GiverPhone,
		GiverGithub: voucherData.GiverGithub,
		ReceiverID:  receiver.ID,
		QuotaList:   quotaResults,
		VoucherCode: req.VoucherCode,
		Operation:   models.OperationTransferIn,
		Amount:      totalAmount,
		Status:      status,
		Message:     message,
	}, nil
}

// AddQuotaForStrategy adds quota for strategy execution
func (s *QuotaService) AddQuotaForStrategy(userID string, amount float64, strategyName string) error {
	// Calculate expiry date (end of this/next month)
	now := time.Now().Truncate(time.Second)
	var expiryDate time.Time

	// If less than 30 days until end of month, set expiry to end of next month
	endOfMonth := time.Date(now.Year(), now.Month()+1, 0, 23, 59, 59, 0, now.Location())
	if endOfMonth.Sub(now).Hours() < 24*30 {
		expiryDate = time.Date(now.Year(), now.Month()+2, 0, 23, 59, 59, 0, now.Location())
	} else {
		expiryDate = endOfMonth
	}

	// Start transaction
	tx := s.db.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Add or update quota
	var quota models.Quota
	err := tx.Where("user_id = ? AND expiry_date = ? AND status = ?",
		userID, expiryDate, models.StatusValid).First(&quota).Error

	if err == gorm.ErrRecordNotFound {
		// Create new quota record
		quota = models.Quota{
			UserID:     userID,
			Amount:     amount,
			ExpiryDate: expiryDate,
			Status:     models.StatusValid,
		}
		if err := tx.Create(&quota).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to create quota: %w", err)
		}
	} else if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to query quota: %w", err)
	} else {
		// Update existing quota
		if err := tx.Model(&quota).Update("amount", quota.Amount+amount).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to update quota: %w", err)
		}
	}

	// Prepare detailed audit information for recharge
	auditDetails := &models.QuotaAuditDetails{
		Operation: models.OperationRecharge,
		Summary: models.QuotaAuditSummary{
			TotalAmount:        amount,
			TotalItems:         1,
			SuccessfulItems:    1,
			EarliestExpiryDate: expiryDate.Format(time.RFC3339),
		},
		Items: []models.QuotaAuditDetailItem{
			{
				Amount:        amount,
				ExpiryDate:    expiryDate.Format(time.RFC3339),
				Status:        models.AuditStatusSuccess,
				OriginalQuota: quota.Amount - amount, // Before recharge
				NewQuota:      quota.Amount,          // After recharge
			},
		},
	}

	// Add strategy information if available
	if strategyName != "" {
		auditDetails.Items[0].FailureReason = fmt.Sprintf("Strategy: %s", strategyName)
	}

	// Record audit log only if it's not expired yet
	auditRecord := &models.QuotaAudit{
		UserID:       userID,
		Amount:       amount,
		Operation:    models.OperationRecharge,
		StrategyName: strategyName,
		ExpiryDate:   expiryDate,
	}
	if err := auditRecord.MarshalDetails(auditDetails); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to marshal audit details: %w", err)
	}
	if err := tx.Create(auditRecord).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create audit record: %w", err)
	}

	// Update AiGateway quota
	if err := s.deltaQuotaInAiGateway(userID, amount); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update AiGateway quota: %w", err)
	}

	tx.Commit()
	return nil
}

// ExpireQuotas expires quotas and synchronizes with AiGateway
func (s *QuotaService) ExpireQuotas() error {
	now := time.Now().Truncate(time.Second)

	// Find expired but still valid quotas
	var expiredQuotas []models.Quota
	if err := s.db.DB.Where("status = ? AND expiry_date < ?", models.StatusValid, now).Find(&expiredQuotas).Error; err != nil {
		return fmt.Errorf("failed to find expired quotas: %w", err)
	}

	if len(expiredQuotas) == 0 {
		return nil
	}

	// Group by user
	userQuotaMap := make(map[string]float64)
	for _, quota := range expiredQuotas {
		userQuotaMap[quota.UserID] += quota.Amount
	}

	// Start transaction
	tx := s.db.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Update status to expired
	if err := tx.Model(&models.Quota{}).
		Where("status = ? AND expiry_date < ?", models.StatusValid, now).
		Update("status", models.StatusExpired).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update quota status: %w", err)
	}

	// Process each user
	for userID := range userQuotaMap {
		// Get user's remaining valid quota
		var validQuotaSum float64
		if err := tx.Model(&models.Quota{}).
			Where("user_id = ? AND status = ?", userID, models.StatusValid).
			Select("COALESCE(SUM(amount), 0)").Scan(&validQuotaSum).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to calculate valid quota for user %s: %w", userID, err)
		}

		// Get current quota info from AiGateway
		totalQuota, err := s.getQuotaFromAiGateway(userID)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to get total quota from AiGateway for user %s: %w", userID, err)
		}

		usedQuota, err := s.getUsedQuotaFromAiGateway(userID)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to get used quota from AiGateway for user %s: %w", userID, err)
		}

		remainingQuota := totalQuota - usedQuota

		// Reset used quota first
		if err := s.deltaUsedQuotaInAiGateway(userID, -usedQuota); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to reset used quota for user %s: %w", userID, err)
		}

		// Adjust total quota
		validQuota := validQuotaSum
		var newTotalQuota float64
		if validQuota >= remainingQuota {
			newTotalQuota = validQuota
		} else {
			newTotalQuota = validQuota
		}

		deltaQuota := newTotalQuota - totalQuota
		if deltaQuota != 0 {
			if err := s.deltaQuotaInAiGateway(userID, deltaQuota); err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to adjust total quota for user %s: %w", userID, err)
			}
		}
	}

	tx.Commit()
	return nil
}

// MergeQuotaRecords merges quota records for the same user and expiry date
func (s *QuotaService) MergeQuotaRecords() error {
	// QuotaGroup represents quota records grouped by user and expiry date
	type QuotaGroup struct {
		UserID      string    `gorm:"column:user_id"`
		ExpiryDate  time.Time `gorm:"column:expiry_date"`
		Status      string    `gorm:"column:status"`
		TotalAmount float64   `gorm:"column:total_amount"`
		RecordCount int       `gorm:"column:record_count"`
	}

	// Find groups with multiple records
	var groups []QuotaGroup
	result := s.db.DB.Model(&models.Quota{}).
		Select("user_id, expiry_date, status, SUM(amount) as total_amount, COUNT(*) as record_count").
		Group("user_id, expiry_date, status").
		Having("COUNT(*) > 1").
		Scan(&groups)

	if result.Error != nil {
		return fmt.Errorf("failed to find quota groups: %w", result.Error)
	}

	// Start transaction
	tx := s.db.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Process each group that has duplicates
	for _, group := range groups {
		// Delete all existing records for this group
		if err := tx.Where("user_id = ? AND expiry_date = ? AND status = ?",
			group.UserID, group.ExpiryDate, group.Status).Delete(&models.Quota{}).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to delete duplicate quota records: %w", err)
		}

		// Create a single merged record (only if total amount is positive)
		if group.TotalAmount > 0 {
			mergedQuota := &models.Quota{
				UserID:     group.UserID,
				Amount:     group.TotalAmount,
				ExpiryDate: group.ExpiryDate,
				Status:     group.Status,
			}
			if err := tx.Create(mergedQuota).Error; err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to create merged quota record: %w", err)
			}
		}
	}

	tx.Commit()
	return nil
}

// Helper methods for AiGateway communication

func (s *QuotaService) getQuotaFromAiGateway(userID string) (float64, error) {
	url := fmt.Sprintf("%s%s?user_id=%s", s.aiGatewayConf.GetBaseURL(), s.aiGatewayConf.AdminPath, userID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// Set admin key header if configured
	if s.aiGatewayConf.AuthHeader != "" && s.aiGatewayConf.AuthValue != "" {
		req.Header.Set(s.aiGatewayConf.AuthHeader, s.aiGatewayConf.AuthValue)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get total quota: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Success bool   `json:"success"`
		Data    struct {
			UserID string  `json:"user_id"`
			Quota  float64 `json:"quota"`
			Type   string  `json:"type"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	if !result.Success {
		return 0, fmt.Errorf("AI Gateway error: %s - %s", result.Code, result.Message)
	}

	return result.Data.Quota, nil
}

func (s *QuotaService) getUsedQuotaFromAiGateway(userID string) (float64, error) {
	url := fmt.Sprintf("%s%s/used?user_id=%s", s.aiGatewayConf.GetBaseURL(), s.aiGatewayConf.AdminPath, userID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// Set admin key header if configured
	if s.aiGatewayConf.AuthHeader != "" && s.aiGatewayConf.AuthValue != "" {
		req.Header.Set(s.aiGatewayConf.AuthHeader, s.aiGatewayConf.AuthValue)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get used quota: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Success bool   `json:"success"`
		Data    struct {
			UserID string  `json:"user_id"`
			Quota  float64 `json:"quota"`
			Type   string  `json:"type"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	if !result.Success {
		return 0, fmt.Errorf("AI Gateway error: %s - %s", result.Code, result.Message)
	}

	return result.Data.Quota, nil
}

func (s *QuotaService) deltaQuotaInAiGateway(userID string, delta float64) error {
	reqURL := fmt.Sprintf("%s%s/delta", s.aiGatewayConf.GetBaseURL(), s.aiGatewayConf.AdminPath)

	data := url.Values{}
	data.Set("user_id", userID)
	data.Set("value", strconv.FormatFloat(delta, 'f', -1, 64))

	req, err := http.NewRequest("POST", reqURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set admin key header if configured
	if s.aiGatewayConf.AuthHeader != "" && s.aiGatewayConf.AuthValue != "" {
		req.Header.Set(s.aiGatewayConf.AuthHeader, s.aiGatewayConf.AuthValue)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delta quota: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Success bool   `json:"success"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("AI Gateway error: %s - %s", result.Code, result.Message)
	}

	return nil
}

func (s *QuotaService) deltaUsedQuotaInAiGateway(userID string, delta float64) error {
	reqURL := fmt.Sprintf("%s%s/used/delta", s.aiGatewayConf.GetBaseURL(), s.aiGatewayConf.AdminPath)

	data := url.Values{}
	data.Set("user_id", userID)
	data.Set("value", strconv.FormatFloat(delta, 'f', -1, 64))

	req, err := http.NewRequest("POST", reqURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set admin key header if configured
	if s.aiGatewayConf.AuthHeader != "" && s.aiGatewayConf.AuthValue != "" {
		req.Header.Set(s.aiGatewayConf.AuthHeader, s.aiGatewayConf.AuthValue)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delta used quota: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Success bool   `json:"success"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("AI Gateway error: %s - %s", result.Code, result.Message)
	}

	return nil
}

// DeltaUsedQuotaInAiGateway is a public wrapper for deltaUsedQuotaInAiGateway
func (s *QuotaService) DeltaUsedQuotaInAiGateway(userID string, delta float64) error {
	return s.deltaUsedQuotaInAiGateway(userID, delta)
}

// GetUserQuotaAuditRecords gets quota audit records for a specific user (admin function)
func (s *QuotaService) GetUserQuotaAuditRecords(userID string, page, pageSize int) ([]QuotaAuditRecord, int64, error) {
	var auditRecords []models.QuotaAudit
	var total int64

	// Get total count
	if err := s.db.Model(&models.QuotaAudit{}).Where("user_id = ?", userID).Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count quota audit records: %w", err)
	}

	// Get records with pagination
	offset := (page - 1) * pageSize
	if err := s.db.Where("user_id = ?", userID).
		Order("create_time DESC").
		Offset(offset).
		Limit(pageSize).
		Find(&auditRecords).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to query quota audit records: %w", err)
	}

	// Convert to response format
	var records []QuotaAuditRecord
	for _, record := range auditRecords {
		auditRecord := QuotaAuditRecord{
			Amount:       record.Amount,
			Operation:    record.Operation,
			VoucherCode:  record.VoucherCode,
			RelatedUser:  record.RelatedUser,
			StrategyName: record.StrategyName,
			ExpiryDate:   record.ExpiryDate,
			CreateTime:   record.CreateTime,
		}

		// Unmarshal details if present
		if record.Details != "" {
			details, err := record.UnmarshalDetails()
			if err == nil {
				auditRecord.Details = details
			}
		}

		records = append(records, auditRecord)
	}

	return records, total, nil
}
