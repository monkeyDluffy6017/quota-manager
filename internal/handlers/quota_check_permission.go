package handlers

import (
	"net/http"
	"quota-manager/internal/services"
	"quota-manager/internal/validation"

	"github.com/gin-gonic/gin"
)

// QuotaCheckPermissionHandler handles quota check permission-related HTTP requests
type QuotaCheckPermissionHandler struct {
	quotaCheckPermissionService *services.QuotaCheckPermissionService
}

// NewQuotaCheckPermissionHandler creates a new quota check permission handler
func NewQuotaCheckPermissionHandler(quotaCheckPermissionService *services.QuotaCheckPermissionService) *QuotaCheckPermissionHandler {
	return &QuotaCheckPermissionHandler{
		quotaCheckPermissionService: quotaCheckPermissionService,
	}
}

// SetUserQuotaCheckRequest represents user quota check request
type SetUserQuotaCheckRequest struct {
	UserID  string `json:"user_id" validate:"required,uuid"`
	Enabled *bool  `json:"enabled" validate:"required"`
}

// SetDepartmentQuotaCheckRequest represents department quota check request
type SetDepartmentQuotaCheckRequest struct {
	DepartmentName string `json:"department_name" validate:"required,department_name"`
	Enabled        *bool  `json:"enabled" validate:"required"`
}

// SetUserQuotaCheckSetting sets quota check setting for a user
func (h *QuotaCheckPermissionHandler) SetUserQuotaCheckSetting(c *gin.Context) {
	var req SetUserQuotaCheckRequest

	if err := validation.ValidateJSON(c, &req); err != nil {
		return
	}

	if err := h.quotaCheckPermissionService.SetUserQuotaCheckSetting(req.UserID, *req.Enabled); err != nil {
		// Check if it's a ServiceError
		if serviceErr, ok := err.(*services.ServiceError); ok {
			switch serviceErr.Code {
			case services.ErrorUserNotFound:
				c.JSON(http.StatusBadRequest, gin.H{
					"code":    "quota_check_permission.user_not_found",
					"message": serviceErr.Message,
					"success": false,
				})
				return
			case services.ErrorDeptNotFound:
				c.JSON(http.StatusBadRequest, gin.H{
					"code":    "quota_check_permission.department_not_found",
					"message": serviceErr.Message,
					"success": false,
				})
				return
			case services.ErrorDatabaseError:
				c.JSON(http.StatusInternalServerError, gin.H{
					"code":    "quota_check_permission.database_error",
					"message": serviceErr.Message,
					"success": false,
				})
				return
			}
		}

		// Default case for other errors
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    "quota_check_permission.set_user_setting_failed",
			"message": "Failed to set user quota check setting: " + err.Error(),
			"success": false,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    "quota_check_permission.set_user_setting_success",
		"message": "User quota check setting set successfully",
		"success": true,
		"data": gin.H{
			"user_id": req.UserID,
			"enabled": *req.Enabled,
		},
	})
}

// SetDepartmentQuotaCheckSetting sets quota check setting for a department
func (h *QuotaCheckPermissionHandler) SetDepartmentQuotaCheckSetting(c *gin.Context) {
	var req SetDepartmentQuotaCheckRequest

	if err := validation.ValidateJSON(c, &req); err != nil {
		return
	}

	if err := h.quotaCheckPermissionService.SetDepartmentQuotaCheckSetting(req.DepartmentName, *req.Enabled); err != nil {
		// Check if it's a ServiceError
		if serviceErr, ok := err.(*services.ServiceError); ok {
			switch serviceErr.Code {
			case services.ErrorUserNotFound:
				c.JSON(http.StatusBadRequest, gin.H{
					"code":    "quota_check_permission.user_not_found",
					"message": serviceErr.Message,
					"success": false,
				})
				return
			case services.ErrorDeptNotFound:
				c.JSON(http.StatusBadRequest, gin.H{
					"code":    "quota_check_permission.department_not_found",
					"message": serviceErr.Message,
					"success": false,
				})
				return
			case services.ErrorDatabaseError:
				c.JSON(http.StatusInternalServerError, gin.H{
					"code":    "quota_check_permission.database_error",
					"message": serviceErr.Message,
					"success": false,
				})
				return
			}
		}

		// Default case for other errors
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    "quota_check_permission.set_department_setting_failed",
			"message": "Failed to set department quota check setting: " + err.Error(),
			"success": false,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    "quota_check_permission.set_department_setting_success",
		"message": "Department quota check setting set successfully",
		"success": true,
		"data": gin.H{
			"department_name": req.DepartmentName,
			"enabled":         *req.Enabled,
		},
	})
}
