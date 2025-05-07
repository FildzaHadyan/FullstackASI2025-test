package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	db     *gorm.DB
	rdb    *redis.Client
	ctx    = context.Background()
	bucket = os.Getenv("S3_BUCKET")
)

// Model
type Client struct {
	ID           uint       `gorm:"primaryKey" json:"id"`
	Name         string     `gorm:"type:char(250);not null" json:"name"`
	Slug         string     `gorm:"type:char(100);not null;uniqueIndex" json:"slug"`
	IsProject    string     `gorm:"type:varchar(30);not null;default:'0'" json:"is_project"`
	SelfCapture  string     `gorm:"type:char(1);not null;default:'1'" json:"self_capture"`
	ClientPrefix string     `gorm:"type:char(4);not null" json:"client_prefix"`
	ClientLogo   string     `gorm:"type:char(255);not null;default:'no-image.jpg'" json:"client_logo"`
	Address      string     `gorm:"type:text" json:"address"`
	PhoneNumber  string     `gorm:"type:char(50)" json:"phone_number"`
	City         string     `gorm:"type:char(50)" json:"city"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	DeletedAt    *time.Time `json:"deleted_at"`
}

func initDB() {
	dsn := os.Getenv("DATABASE_DSN")
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalln("Failed connect to DB:", err)
	}
	db.AutoMigrate(&Client{})
}

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Password: os.Getenv("REDIS_PASS"),
		DB:       0,
	})
}

func initS3Uploader() *manager.Uploader {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalln("Unable to load AWS config:", err)
	}
	client := s3.NewFromConfig(cfg)
	return manager.NewUploader(client)
}

func main() {
	initDB()
	initRedis()
	uploader := initS3Uploader()

	r := gin.Default()

	r.POST("/clients", func(c *gin.Context) {
		var input Client
		if err := c.ShouldBind(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// handle file upload
		file, err := c.FormFile("client_logo")
		if err == nil {
			src, _ := file.Open()
			key := uuid.New().String() + "-" + file.Filename
			_, err = uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key), Body: src,
			})
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "upload S3 failed"})
				return
			}
			input.ClientLogo = fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucket, key)
		}
		// simpan ke database
		if err := db.Create(&input).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// tambahkan ke Redis
		data, _ := json.Marshal(input)
		rdb.Set(ctx, input.Slug, data, 0)
		c.JSON(http.StatusCreated, input)
	})

	r.GET("/clients/:slug", func(c *gin.Context) {
		slug := c.Param("slug")
		// dicoba apakah ada di Redis
		if val, err := rdb.Get(ctx, slug).Result(); err == nil {
			var cached Client
			json.Unmarshal([]byte(val), &cached)
			c.JSON(http.StatusOK, cached)
			return
		}
		// dikembalikan ke database
		var client Client
		if err := db.Where("slug = ? AND deleted_at IS NULL", slug).First(&client).Error; err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		// cache
		data, _ := json.Marshal(client)
		rdb.Set(ctx, slug, data, 0)
		c.JSON(http.StatusOK, client)
	})

	r.PUT("/clients/:slug", func(c *gin.Context) {
		slug := c.Param("slug")
		var client Client
		if err := db.Where("slug = ? AND deleted_at IS NULL", slug).First(&client).Error; err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		var input Client
		if err := c.ShouldBind(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// menghapus dari Redis
		rdb.Del(ctx, slug)
		// update fields
		db.Model(&client).Updates(input)
		// meng-generate Redis kembali
		data, _ := json.Marshal(client)
		rdb.Set(ctx, client.Slug, data, 0)
		c.JSON(http.StatusOK, client)
	})

	r.DELETE("/clients/:slug", func(c *gin.Context) {
		slug := c.Param("slug")
		var client Client
		if err := db.Where("slug = ? AND deleted_at IS NULL", slug).First(&client).Error; err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		now := time.Now()
		db.Model(&client).Update("deleted_at", &now)
		// menghapus cache
		rdb.Del(ctx, slug)
		c.Status(http.StatusNoContent)
	})

	r.Run() // listens on :8080
}
