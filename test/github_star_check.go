package main

import (
	"fmt"
	"reflect"
	"time"

	"quota-manager/internal/models"
	"quota-manager/internal/services"
)

// testGithubStarCheckEnabledUserStarred tests quota transfer when GitHub star check is enabled and user has starred the required repository
func testGithubStarCheckEnabledUserStarred(ctx *TestContext) TestResult {
	// Enable GitHub star check using reflection
	quotaService := ctx.QuotaService
	configValue := reflect.ValueOf(quotaService).Elem().FieldByName("config")
	if configValue.IsValid() && configValue.CanSet() {
		githubStarCheck := configValue.Elem().FieldByName("GithubStarCheck")
		if githubStarCheck.IsValid() {
			enabled := githubStarCheck.FieldByName("Enabled")
			requiredRepo := githubStarCheck.FieldByName("RequiredRepo")
			if enabled.IsValid() && enabled.CanSet() {
				enabled.SetBool(true)
			}
			if requiredRepo.IsValid() && requiredRepo.CanSet() {
				requiredRepo.SetString("test-org/test-repo")
			}
		}
	}

	// Create test users
	fromUser := createTestUser("starred_user", "Starred User", 0)
	fromUser.GithubStar = "test-org/test-repo,other-org/other-repo"
	if err := ctx.DB.AuthDB.Create(fromUser).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create from user failed: %v", err)}
	}

	toUser := createTestUser("recipient_user", "Recipient User", 0)
	if err := ctx.DB.AuthDB.Create(toUser).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create to user failed: %v", err)}
	}

	// Setup initial quota with specific expiry date matching the transfer request
	transferExpiryDate := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)
	quotaRecord := &models.Quota{
		UserID:     fromUser.ID,
		Amount:     100,
		ExpiryDate: transferExpiryDate,
		Status:     models.StatusValid,
	}
	if err := ctx.DB.DB.Create(quotaRecord).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create quota record failed: %v", err)}
	}

	// Also set quota in mock store to ensure AiGateway returns correct values
	mockStore.SetQuota(fromUser.ID, 100)
	mockStore.SetUsed(fromUser.ID, 0)

	// Test: Attempt transfer
	transferReq := &services.TransferOutRequest{
		ReceiverID: toUser.ID,
		QuotaList: []services.TransferQuotaItem{
			{Amount: 50, ExpiryDate: transferExpiryDate},
		},
	}

	// Debug: Print user info before transfer
	fmt.Printf("DEBUG: User %s has GithubStar: %s\n", fromUser.ID, fromUser.GithubStar)

	_, err := ctx.QuotaService.TransferOut(&models.AuthUser{
		ID: fromUser.ID, Name: fromUser.Name, Phone: "13800138000", Github: fromUser.GithubID,
	}, transferReq)

	if err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Transfer should succeed but failed: %v", err)}
	}

	return TestResult{Passed: true, Message: "GitHub star check enabled - user starred test passed"}
}

// testGithubStarCheckEnabledUserNotStarred tests quota transfer when GitHub star check is enabled and user has NOT starred the required repository
func testGithubStarCheckEnabledUserNotStarred(ctx *TestContext) TestResult {
	// Enable GitHub star check using reflection
	quotaService := ctx.QuotaService
	configValue := reflect.ValueOf(quotaService).Elem().FieldByName("config")
	if configValue.IsValid() && configValue.CanSet() {
		githubStarCheck := configValue.Elem().FieldByName("GithubStarCheck")
		if githubStarCheck.IsValid() {
			enabled := githubStarCheck.FieldByName("Enabled")
			requiredRepo := githubStarCheck.FieldByName("RequiredRepo")
			if enabled.IsValid() && enabled.CanSet() {
				enabled.SetBool(true)
			}
			if requiredRepo.IsValid() && requiredRepo.CanSet() {
				requiredRepo.SetString("test-org/test-repo")
			}
		}
	}

	// Create test users
	fromUser := createTestUser("not_starred_user", "Not Starred User", 0)
	fromUser.GithubStar = "other-org/other-repo,different-org/different-repo"
	if err := ctx.DB.AuthDB.Create(fromUser).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create from user failed: %v", err)}
	}

	toUser := createTestUser("recipient_user2", "Recipient User 2", 0)
	if err := ctx.DB.AuthDB.Create(toUser).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create to user failed: %v", err)}
	}

	// Setup initial quota with specific expiry date matching the transfer request
	transferExpiryDate := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)
	quotaRecord := &models.Quota{
		UserID:     fromUser.ID,
		Amount:     100,
		ExpiryDate: transferExpiryDate,
		Status:     models.StatusValid,
	}
	if err := ctx.DB.DB.Create(quotaRecord).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create quota record failed: %v", err)}
	}

	// Also set quota in mock store to ensure AiGateway returns correct values
	mockStore.SetQuota(fromUser.ID, 100)
	mockStore.SetUsed(fromUser.ID, 0)

	// Test: Attempt transfer
	transferReq := &services.TransferOutRequest{
		ReceiverID: toUser.ID,
		QuotaList: []services.TransferQuotaItem{
			{Amount: 50, ExpiryDate: transferExpiryDate},
		},
	}

	// Debug: Print user info before transfer
	fmt.Printf("DEBUG: User %s has GithubStar: %s\n", fromUser.ID, fromUser.GithubStar)

	_, err := ctx.QuotaService.TransferOut(&models.AuthUser{
		ID: fromUser.ID, Name: fromUser.Name, Phone: "13800138000", Github: fromUser.GithubID,
	}, transferReq)

	if err == nil {
		return TestResult{Passed: false, Message: "Transfer should fail but succeeded"}
	}

	// Check if it's the expected error
	if err.Error() != "GitHub star required: test-org/test-repo" {
		return TestResult{Passed: false, Message: fmt.Sprintf("Expected GitHub star required error, got: %v", err)}
	}

	return TestResult{Passed: true, Message: "GitHub star check enabled - user not starred test passed"}
}

// testGithubStarCheckDisabled tests quota transfer when GitHub star check is disabled
func testGithubStarCheckDisabled(ctx *TestContext) TestResult {
	// Disable GitHub star check using reflection
	quotaService := ctx.QuotaService
	configValue := reflect.ValueOf(quotaService).Elem().FieldByName("config")
	if configValue.IsValid() && configValue.CanSet() {
		githubStarCheck := configValue.Elem().FieldByName("GithubStarCheck")
		if githubStarCheck.IsValid() {
			enabled := githubStarCheck.FieldByName("Enabled")
			requiredRepo := githubStarCheck.FieldByName("RequiredRepo")
			if enabled.IsValid() && enabled.CanSet() {
				enabled.SetBool(false)
			}
			if requiredRepo.IsValid() && requiredRepo.CanSet() {
				requiredRepo.SetString("test-org/test-repo")
			}
		}
	}

	// Create test users - one with star, one without
	fromUser1 := createTestUser("starred_user_disabled", "Starred User Disabled", 0)
	fromUser1.GithubStar = "test-org/test-repo"
	if err := ctx.DB.AuthDB.Create(fromUser1).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create starred user failed: %v", err)}
	}

	fromUser2 := createTestUser("not_starred_user_disabled", "Not Starred User Disabled", 0)
	fromUser2.GithubStar = "other-org/other-repo"
	if err := ctx.DB.AuthDB.Create(fromUser2).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create not starred user failed: %v", err)}
	}

	toUser := createTestUser("recipient_user3", "Recipient User 3", 0)
	if err := ctx.DB.AuthDB.Create(toUser).Error; err != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Create to user failed: %v", err)}
	}

	// Setup initial quota for both users
	mockStore.SetQuota(fromUser1.ID, 100)
	mockStore.SetQuota(fromUser2.ID, 100)

	// Test: Attempt transfers for both users
	transferReq := &services.TransferOutRequest{
		ReceiverID: toUser.ID,
		QuotaList: []services.TransferQuotaItem{
			{Amount: 50, ExpiryDate: time.Now().Add(30 * 24 * time.Hour)},
		},
	}

	// Test starred user
	_, err1 := ctx.QuotaService.TransferOut(&models.AuthUser{
		ID: fromUser1.ID, Name: fromUser1.Name, Phone: "13800138000", Github: fromUser1.GithubID,
	}, transferReq)
	if err1 != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Starred user transfer should succeed but failed: %v", err1)}
	}

	// Test not starred user
	_, err2 := ctx.QuotaService.TransferOut(&models.AuthUser{
		ID: fromUser2.ID, Name: fromUser2.Name, Phone: "13800138001", Github: fromUser2.GithubID,
	}, transferReq)
	if err2 != nil {
		return TestResult{Passed: false, Message: fmt.Sprintf("Not starred user transfer should succeed but failed: %v", err2)}
	}

	return TestResult{Passed: true, Message: "GitHub star check disabled test passed"}
}
