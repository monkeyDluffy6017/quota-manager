package services

import (
	"fmt"
	"quota-manager/internal/database"
	"quota-manager/internal/models"
	"quota-manager/pkg/logger"

	"go.uber.org/zap"
)

// UserConversionService handles conversion between employee_number and user_id
type UserConversionService struct {
	db *database.DB
}

// NewUserConversionService creates a new user conversion service
func NewUserConversionService(db *database.DB) *UserConversionService {
	return &UserConversionService{
		db: db,
	}
}

// GetUserIDByEmployeeNumber converts employee_number to user_id
func (s *UserConversionService) GetUserIDByEmployeeNumber(employeeNumber string) (string, error) {
	if employeeNumber == "" {
		return "", fmt.Errorf("employee_number cannot be empty")
	}

	var user models.UserInfo
	err := s.db.AuthDB.Where("employee_number = ?", employeeNumber).First(&user).Error
	if err != nil {
		logger.Logger.Warn("Failed to find user by employee_number",
			zap.String("employee_number", employeeNumber),
			zap.Error(err))
		return "", fmt.Errorf("user not found for employee_number: %s", employeeNumber)
	}

	return user.ID, nil
}

// GetEmployeeNumberByUserID converts user_id to employee_number
func (s *UserConversionService) GetEmployeeNumberByUserID(userID string) (string, error) {
	if userID == "" {
		return "", fmt.Errorf("user_id cannot be empty")
	}

	var user models.UserInfo
	err := s.db.AuthDB.Where("id = ?", userID).First(&user).Error
	if err != nil {
		logger.Logger.Warn("Failed to find user by user_id",
			zap.String("user_id", userID),
			zap.Error(err))
		return "", fmt.Errorf("user not found for user_id: %s", userID)
	}

	return user.EmployeeNumber, nil
}

// BatchGetUserIDsByEmployeeNumbers converts multiple employee_numbers to user_ids
func (s *UserConversionService) BatchGetUserIDsByEmployeeNumbers(employeeNumbers []string) (map[string]string, error) {
	if len(employeeNumbers) == 0 {
		return make(map[string]string), nil
	}

	var users []models.UserInfo
	err := s.db.AuthDB.Where("employee_number IN ?", employeeNumbers).Find(&users).Error
	if err != nil {
		logger.Logger.Error("Failed to batch query users by employee_numbers",
			zap.Strings("employee_numbers", employeeNumbers),
			zap.Error(err))
		return nil, fmt.Errorf("failed to batch query users: %w", err)
	}

	result := make(map[string]string)
	for _, user := range users {
		result[user.EmployeeNumber] = user.ID
	}

	return result, nil
}

// BatchGetEmployeeNumbersByUserIDs converts multiple user_ids to employee_numbers
func (s *UserConversionService) BatchGetEmployeeNumbersByUserIDs(userIDs []string) (map[string]string, error) {
	if len(userIDs) == 0 {
		return make(map[string]string), nil
	}

	var users []models.UserInfo
	err := s.db.AuthDB.Where("id IN ?", userIDs).Find(&users).Error
	if err != nil {
		logger.Logger.Error("Failed to batch query users by user_ids",
			zap.Strings("user_ids", userIDs),
			zap.Error(err))
		return nil, fmt.Errorf("failed to batch query users: %w", err)
	}

	result := make(map[string]string)
	for _, user := range users {
		result[user.ID] = user.EmployeeNumber
	}

	return result, nil
}
