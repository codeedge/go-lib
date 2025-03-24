package jwt

import (
	"errors"
	"fmt"
	"time"

	"github.com/gogf/gf/frame/g"

	"github.com/golang-jwt/jwt"
)

type JWT struct {
	SigningKey []byte
}

var (
	TokenExpired     = errors.New("token is expired")
	TokenNotValidYet = errors.New("token not active yet")
	TokenMalformed   = errors.New("that's not even a token")
	TokenInvalid     = errors.New("couldn't handle this token")
	SignKey          = "default-signKey"
)

// Info .
type Info struct {
	UID        int64
	OpenID     string
	UnionID    string
	SessionKey string
	Extend     interface{} // 扩展字段
}

type CustomClaims struct {
	*Info
	jwt.StandardClaims
}

func NewJWT() *JWT {
	return &JWT{
		[]byte(GetSignKey()),
	}
}
func GetSignKey() string {
	return SignKey
}
func SetSignKey(key string) string {
	SignKey = key
	return SignKey
}

func (j *JWT) ParseToken(tokenString string) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return j.SigningKey, nil
	})
	if err != nil {
		if ve, ok := err.(*jwt.ValidationError); ok {
			if ve.Errors&jwt.ValidationErrorMalformed != 0 {
				return nil, TokenMalformed
			} else if ve.Errors&jwt.ValidationErrorExpired != 0 {
				// Token is expired
				return nil, TokenExpired
			} else if ve.Errors&jwt.ValidationErrorNotValidYet != 0 {
				return nil, TokenNotValidYet
			} else {
				return nil, TokenInvalid
			}
		}
	}
	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, nil
	}
	return nil, TokenInvalid
}

func (j *JWT) RefreshToken(tokenString string) (string, error) {
	jwt.TimeFunc = func() time.Time {
		return time.Unix(0, 0)
	}
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return j.SigningKey, nil
	})
	if err != nil {
		return "", err
	}
	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		jwt.TimeFunc = time.Now
		claims.StandardClaims.ExpiresAt = time.Now().Add(24 * time.Hour).Unix()
		return j.CreateToken(*claims)
	}
	return "", TokenInvalid
}

func (j *JWT) CreateToken(claims CustomClaims) (string, error) {
	claims.StandardClaims.ExpiresAt = time.Now().Add(24 * time.Hour * 30).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tk, err := token.SignedString(j.SigningKey)
	if err != nil {
		g.Log().Errorf("CreateToken失败:%s", err.Error())
	}
	fmt.Println("Token:", tk)
	return tk, err
}

func GenerateToken(info *Info) string {
	claim := CustomClaims{
		Info: info,
	}
	tokenStr, _ := NewJWT().CreateToken(claim)
	return tokenStr
}
