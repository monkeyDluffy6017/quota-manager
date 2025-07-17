package services

import (
	"encoding/json"
	"fmt"
	"quota-manager/internal/config"
	"quota-manager/internal/database"
	"quota-manager/internal/models"
	"quota-manager/pkg/logger"

	"go.uber.org/zap"
)

// QuotaCheckPermissionService handles quota check permission management
type QuotaCheckPermissionService struct {
	db                    *database.DB
	aiGatewayConf         *config.AiGatewayConfig
	employeeSyncConf      *config.EmployeeSyncConfig
	higressClient         HigressQuotaCheckClient
	userConversionService *UserConversionService
}

// HigressQuotaCheckClient interface for Higress quota check permission management
type HigressQuotaCheckClient interface {
	SetUserQuotaCheckPermission(userID string, enabled bool) error
}

// NewQuotaCheckPermissionService creates a new quota check permission service
func NewQuotaCheckPermissionService(db *database.DB, aiGatewayConf *config.AiGatewayConfig, employeeSyncConf *config.EmployeeSyncConfig, higressClient HigressQuotaCheckClient, userConversionService *UserConversionService) *QuotaCheckPermissionService {
	return &QuotaCheckPermissionService{
		db:                    db,
		aiGatewayConf:         aiGatewayConf,
		employeeSyncConf:      employeeSyncConf,
		higressClient:         higressClient,
		userConversionService: userConversionService,
	}
}

// SetUserQuotaCheckSetting sets quota check setting for a user
func (s *QuotaCheckPermissionService) SetUserQuotaCheckSetting(userID string, enabled bool) error {
	// Check if user exists in auth_users table
	var user models.UserInfo
	err := s.db.AuthDB.Where("id = ?", userID).First(&user).Error
	if err != nil {
		return NewUserNotFoundError(userID)
	}

	// Check if setting already exists
	var setting models.QuotaCheckSetting
	err = s.db.DB.Where("target_type = ? AND target_identifier = ?",
		models.TargetTypeUser, userID).First(&setting).Error

	if err == nil {
		// Check if setting is the same
		if setting.Enabled == enabled {
			// Setting already exists with same value - this is ok (idempotent operation)
			return nil
		}

		// Update existing setting
		setting.Enabled = enabled
		if err := s.db.DB.Save(&setting).Error; err != nil {
			return NewDatabaseError("update quota check setting", err)
		}
	} else {
		// Create new setting
		setting = models.QuotaCheckSetting{
			TargetType:       models.TargetTypeUser,
			TargetIdentifier: userID,
			Enabled:          enabled,
		}
		if err := s.db.DB.Create(&setting).Error; err != nil {
			return NewDatabaseError("create quota check setting", err)
		}
	}

	// Update employee quota check permissions using employee_number for department lookup
	if err := s.UpdateEmployeeQuotaCheckPermissions(user.EmployeeNumber); err != nil {
		logger.Logger.Error("Failed to update user quota check permissions",
			zap.String("user_id", userID),
			zap.String("employee_number", user.EmployeeNumber),
			zap.Error(err))
		// Continue execution - setting is already saved
	}

	// Record audit
	auditDetails := map[string]interface{}{
		"user_id": userID,
		"enabled": enabled,
	}
	s.recordAudit(models.OperationQuotaCheckSet, models.TargetTypeUser, userID, auditDetails)

	return nil
}

// SetDepartmentQuotaCheckSetting sets quota check setting for a department
func (s *QuotaCheckPermissionService) SetDepartmentQuotaCheckSetting(departmentName string, enabled bool) error {
	// Validate department exists - check if any employee belongs to this department
	var employeeCount int64
	err := s.db.DB.Model(&models.EmployeeDepartment{}).Where("dept_full_level_names LIKE ?", "%"+departmentName+"%").Count(&employeeCount).Error
	if err != nil {
		return NewDatabaseError("validate department existence", err)
	}

	if employeeCount == 0 {
		return NewDepartmentNotFoundError(departmentName)
	}

	// Check if setting already exists
	var setting models.QuotaCheckSetting
	err = s.db.DB.Where("target_type = ? AND target_identifier = ?",
		models.TargetTypeDepartment, departmentName).First(&setting).Error

	if err == nil {
		// Check if setting is the same
		if setting.Enabled == enabled {
			// Setting already exists with same value - this is ok (idempotent operation)
			return nil
		}

		// Update existing setting
		setting.Enabled = enabled
		if err := s.db.DB.Save(&setting).Error; err != nil {
			return NewDatabaseError("update quota check setting", err)
		}
	} else {
		// Create new setting
		setting = models.QuotaCheckSetting{
			TargetType:       models.TargetTypeDepartment,
			TargetIdentifier: departmentName,
			Enabled:          enabled,
		}
		if err := s.db.DB.Create(&setting).Error; err != nil {
			return NewDatabaseError("create quota check setting", err)
		}
	}

	// Update permissions for all employees in this department
	if err := s.UpdateDepartmentQuotaCheckPermissions(departmentName); err != nil {
		logger.Logger.Error("Failed to update department quota check permissions",
			zap.String("department_name", departmentName),
			zap.Error(err))
		// Continue execution - setting is already saved
	}

	// Record audit
	auditDetails := map[string]interface{}{
		"department_name": departmentName,
		"enabled":         enabled,
	}
	s.recordAudit(models.OperationQuotaCheckSet, models.TargetTypeDepartment, departmentName, auditDetails)

	return nil
}

// GetUserEffectiveQuotaCheckSetting gets effective quota check setting for a user
func (s *QuotaCheckPermissionService) GetUserEffectiveQuotaCheckSetting(employeeNumber string) (bool, error) {
	// Get effective setting directly, no need to check if employee exists
	var effectiveSetting models.EffectiveQuotaCheckSetting
	err := s.db.DB.Where("user_id = ?", employeeNumber).First(&effectiveSetting).Error
	if err != nil {
		return false, nil // Return default (disabled) if no setting found
	}

	return effectiveSetting.Enabled, nil
}

// GetDepartmentQuotaCheckSetting gets quota check setting for a department
func (s *QuotaCheckPermissionService) GetDepartmentQuotaCheckSetting(departmentName string) (bool, error) {
	var setting models.QuotaCheckSetting
	err := s.db.DB.Where("target_type = ? AND target_identifier = ?",
		models.TargetTypeDepartment, departmentName).First(&setting).Error
	if err != nil {
		return false, nil // Return default (disabled) if no setting found
	}

	return setting.Enabled, nil
}

// UpdateEmployeeQuotaCheckPermissions updates effective quota check settings for an employee
func (s *QuotaCheckPermissionService) UpdateEmployeeQuotaCheckPermissions(employeeNumber string) error {
	// First, get user_id from auth_users table
	var user models.UserInfo
	err := s.db.AuthDB.Where("employee_number = ?", employeeNumber).First(&user).Error
	if err != nil {
		// User doesn't exist in auth_users table, skip processing
		return nil
	}
	userID := user.ID

	// Get employee info (optional for non-existent users)
	var employee models.EmployeeDepartment
	var departments []string

	err = s.db.DB.Where("employee_number = ?", employeeNumber).First(&employee).Error
	if err != nil {
		// Employee doesn't exist, use empty department list
		departments = []string{}
	} else {
		// Employee exists, use their department hierarchy
		departments = employee.GetDeptFullLevelNamesAsSlice()
	}

	// Get current effective setting from database (if exists)
	var currentEnabled bool
	var existingEffectiveSetting models.EffectiveQuotaCheckSetting
	err = s.db.DB.Where("user_id = ?", userID).First(&existingEffectiveSetting).Error
	if err == nil {
		currentEnabled = existingEffectiveSetting.Enabled
	} else {
		// No existing effective setting, treat as default (disabled)
		currentEnabled = false
	}

	// Calculate new effective setting
	newEnabled, settingID := s.calculateEffectiveQuotaCheckSetting(userID, departments)

	// Check if setting has actually changed
	settingChanged := currentEnabled != newEnabled

	// For new users (no existing effective setting record), only notify if they have explicit setting
	isNewUser := err != nil
	hasNewSetting := settingID != nil // only true if there's an explicit setting

	// Update or create effective setting in database
	if err == nil {
		// Update existing record
		existingEffectiveSetting.Enabled = newEnabled
		existingEffectiveSetting.SettingID = settingID
		if err := s.db.DB.Save(&existingEffectiveSetting).Error; err != nil {
			return fmt.Errorf("failed to update effective quota check setting: %w", err)
		}
	} else {
		// Create new record
		effectiveSetting := models.EffectiveQuotaCheckSetting{
			UserID:    userID,
			Enabled:   newEnabled,
			SettingID: settingID,
		}
		if err := s.db.DB.Create(&effectiveSetting).Error; err != nil {
			return fmt.Errorf("failed to create effective quota check setting: %w", err)
		}
	}

	// Determine if we should notify Higress
	shouldNotify := false
	notificationReason := ""

	if !isNewUser && settingChanged {
		// Existing user with setting changes
		shouldNotify = true
		if currentEnabled && !newEnabled {
			notificationReason = "quota_check_disabled"
		} else if !currentEnabled && newEnabled {
			notificationReason = "quota_check_enabled"
		}
	} else if isNewUser && hasNewSetting {
		// New user with explicit quota check setting
		shouldNotify = true
		if newEnabled {
			notificationReason = "new_user_quota_check_enabled"
		} else {
			notificationReason = "new_user_quota_check_disabled"
		}
	}

	// Notify Higress if needed
	if shouldNotify && s.higressClient != nil {
		// Convert employee_number back to user_id for Higress API
		userID, err := s.ConvertEmployeeNumberToUserID(employeeNumber)
		if err != nil {
			logger.Logger.Error("Failed to convert employee_number to user_id for Higress call",
				zap.String("employee_number", employeeNumber),
				zap.Error(err))
			// Don't return error - setting is already saved in database
		} else {
			if err := s.higressClient.SetUserQuotaCheckPermission(userID, newEnabled); err != nil {
				logger.Logger.Error("Failed to notify Higress about quota check setting change",
					zap.String("employee_number", employeeNumber),
					zap.String("user_id", userID),
					zap.Bool("new_enabled", newEnabled),
					zap.String("reason", notificationReason),
					zap.Error(err))
				// Don't return error - setting is already saved in database
			} else {
				logger.Logger.Info("Successfully notified Higress about quota check setting change",
					zap.String("employee_number", employeeNumber),
					zap.String("user_id", userID),
					zap.Bool("new_enabled", newEnabled),
					zap.String("reason", notificationReason))
			}
		}
	}

	// Record audit
	auditDetails := map[string]interface{}{
		"employee_number": employeeNumber,
		"enabled":         newEnabled,
		"setting_changed": settingChanged,
		"reason":          notificationReason,
	}
	s.recordAudit(models.OperationQuotaCheckSettingUpdate, models.TargetTypeUser, employeeNumber, auditDetails)

	return nil
}

// UpdateDepartmentQuotaCheckPermissions updates permissions for all employees in a department
func (s *QuotaCheckPermissionService) UpdateDepartmentQuotaCheckPermissions(departmentName string) error {
	// Get all employees in this department
	var employees []models.EmployeeDepartment
	err := s.db.DB.Where("dept_full_level_names LIKE ?", "%"+departmentName+"%").Find(&employees).Error
	if err != nil {
		return fmt.Errorf("failed to get employees in department: %w", err)
	}

	// Update permissions for each employee
	for _, employee := range employees {
		if err := s.UpdateEmployeeQuotaCheckPermissions(employee.EmployeeNumber); err != nil {
			logger.Logger.Error("Failed to update quota check permissions for employee",
				zap.String("employee_number", employee.EmployeeNumber),
				zap.String("department_name", departmentName),
				zap.Error(err))
		}
	}

	return nil
}

// calculateEffectiveQuotaCheckSetting calculates effective quota check setting for a user
func (s *QuotaCheckPermissionService) calculateEffectiveQuotaCheckSetting(userID string, departments []string) (bool, *int) {
	// Priority: User setting > Department setting (most specific department first)
	// Default: disabled (false)

	// Check user setting first
	var userSetting models.QuotaCheckSetting
	err := s.db.DB.Where("target_type = ? AND target_identifier = ?",
		models.TargetTypeUser, userID).First(&userSetting).Error
	if err == nil {
		return userSetting.Enabled, &userSetting.ID
	}

	// Check department settings (from most specific to most general)
	for i := len(departments) - 1; i >= 0; i-- {
		var deptSetting models.QuotaCheckSetting
		err := s.db.DB.Where("target_type = ? AND target_identifier = ?",
			models.TargetTypeDepartment, departments[i]).First(&deptSetting).Error
		if err == nil {
			return deptSetting.Enabled, &deptSetting.ID
		}
	}

	// No setting found, return default (disabled)
	return false, nil
}

// slicesEqual compares two string slices for equality
func (s *QuotaCheckPermissionService) slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// RemoveUserCompletely removes all quota check data associated with a user when they are deleted
func (s *QuotaCheckPermissionService) RemoveUserCompletely(employeeNumber string) error {
	logger.Logger.Info("Removing all quota check data for user",
		zap.String("employee_number", employeeNumber))

	// First, get user_id from auth_users table
	var user models.UserInfo
	err := s.db.AuthDB.Where("employee_number = ?", employeeNumber).First(&user).Error
	var userID string
	if err == nil {
		userID = user.ID
	}

	// Remove user quota check setting - use userID for target_identifier if available, otherwise employeeNumber
	targetIdentifier := employeeNumber
	if userID != "" {
		targetIdentifier = userID
	}
	if err := s.db.DB.Where("target_type = ? AND target_identifier = ?",
		models.TargetTypeUser, targetIdentifier).Delete(&models.QuotaCheckSetting{}).Error; err != nil {
		logger.Logger.Error("Failed to remove user quota check setting",
			zap.String("employee_number", employeeNumber),
			zap.String("target_identifier", targetIdentifier),
			zap.Error(err))
		// Continue with other cleanup even if this fails
	}

	// Remove effective quota check setting - use userID if available
	if userID != "" {
		if err := s.db.DB.Where("user_id = ?", userID).Delete(&models.EffectiveQuotaCheckSetting{}).Error; err != nil {
			logger.Logger.Error("Failed to remove effective quota check setting",
				zap.String("employee_number", employeeNumber),
				zap.String("user_id", userID),
				zap.Error(err))
			// Continue with other cleanup even if this fails
		}
	}

	logger.Logger.Info("Successfully removed quota check data for user",
		zap.String("employee_number", employeeNumber))

	return nil
}

// recordAudit records an audit log entry
func (s *QuotaCheckPermissionService) recordAudit(operation, targetType, targetIdentifier string, details map[string]interface{}) {
	detailsJSON, _ := json.Marshal(details)
	audit := &models.PermissionAudit{
		Operation:        operation,
		TargetType:       targetType,
		TargetIdentifier: targetIdentifier,
		Details:          string(detailsJSON),
	}

	if err := s.db.DB.Create(audit).Error; err != nil {
		logger.Logger.Error("Failed to record audit", zap.Error(err))
	}
}

// ConvertUserIDToEmployeeNumber converts user_id to employee_number for backward compatibility
func (s *QuotaCheckPermissionService) ConvertUserIDToEmployeeNumber(userID string) (string, error) {
	if s.userConversionService == nil {
		return "", fmt.Errorf("user conversion service not initialized")
	}
	return s.userConversionService.GetEmployeeNumberByUserID(userID)
}

// ConvertEmployeeNumberToUserID converts employee_number to user_id for forward compatibility
func (s *QuotaCheckPermissionService) ConvertEmployeeNumberToUserID(employeeNumber string) (string, error) {
	if s.userConversionService == nil {
		return "", fmt.Errorf("user conversion service not initialized")
	}
	return s.userConversionService.GetUserIDByEmployeeNumber(employeeNumber)
}
