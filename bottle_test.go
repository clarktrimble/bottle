package bottle_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"

	. "github.com/clarktrimble/bottle"
)

//go:generate moq -out mock_test.go -pkg bottle_test . Logger Record

// Todo: this is a nice start, now go for 80/20 woot

func TestColSpec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bottle Suite")
}

var _ = Describe("Bottle", func() {
	var (
		ctx       context.Context
		path      string
		btl       *Bottle
		record001 Record
		err       error
	)

	BeforeEach(func() {
		ctx = context.Background()
		lgr := &LoggerMock{
			ErrorFunc: func(ctx context.Context, msg string, err error, kv ...any) {},
			InfoFunc:  func(ctx context.Context, msg string, kv ...any) {},
		}
		record001, _ = mockFactory([]byte("001"))
		path = filepath.Join(GinkgoT().TempDir(), "unit.db")
		btl, err = New(mockFactory, path, lgr)
		Expect(err).ToNot(HaveOccurred())
		err = btl.Flush()
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		btl.Close()
	})

	Describe("creating a bottle", func() {
		When("the record factory is nil", func() {
			It("returns an error", func() {
				btl, err := New(nil, path, nil)

				Expect(err).To(MatchError("record factory is required"))
				Expect(btl).To(BeNil())
			})
		})
	})

	Describe("upserting records", func() {
		var (
			created bool
		)

		JustBeforeEach(func() {
			created, err = btl.Upsert(ctx, record001)
		})

		When("a new record", func() {

			It("creates", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(created).To(BeTrue())

				records, err := btl.All()
				Expect(err).ToNot(HaveOccurred())
				Expect(records).To(HaveLen(1))
			})
		})

		When("an existing record", func() {
			BeforeEach(func() {
				_, _ = btl.Upsert(ctx, record001)
			})

			It("updates", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(created).To(BeFalse())

				records, err := btl.All()
				Expect(err).ToNot(HaveOccurred())
				Expect(records).To(HaveLen(1))
			})
		})

		When("the logger is nil", func() {
			BeforeEach(func() {
				btl.Close()
				btl, err = New(mockFactory, path, nil)
				Expect(err).ToNot(HaveOccurred())
				_, _ = btl.Upsert(ctx, record001)
			})

			It("uses a no-op logger", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(created).To(BeFalse())
			})
		})

		When("distinct records", func() {
			BeforeEach(func() {
				record000, _ := mockFactory([]byte("000"))
				_, _ = btl.Upsert(ctx, record000)
			})

			It("creates distinct", func() {
				Expect(err).ToNot(HaveOccurred())

				records, err := btl.All()
				Expect(err).ToNot(HaveOccurred())
				Expect(records).To(HaveLen(2))
			})
		})
	})

	Describe("getting records", func() {
		When("the record exists", func() {
			BeforeEach(func() {
				_, _ = btl.Upsert(ctx, record001)
			})

			It("returns the record", func() {
				record, err := btl.Get([]byte("test001"))

				Expect(err).ToNot(HaveOccurred())
				Expect(record).ToNot(BeNil())
				Expect(record.Id()).To(Equal("test001"))
			})
		})

		When("the record does not exist", func() {
			It("returns nil", func() {
				record, err := btl.Get([]byte("missing"))

				Expect(err).ToNot(HaveOccurred())
				Expect(record).To(BeNil())
			})
		})
	})

	Describe("expiring records", func() {
		var (
			expired []Record
		)

		JustBeforeEach(func() {
			expired, err = btl.Expire(ctx, baseTime.Add(3*time.Minute))
		})

		When("half are olden", func() {
			BeforeEach(func() {
				record002, _ := mockFactory([]byte("002"))
				_, _ = btl.Upsert(ctx, record002)
				record004, _ := mockFactory([]byte("004"))
				_, _ = btl.Upsert(ctx, record004)
			})

			It("expires half", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(expired).To(HaveLen(1))
				Expect(expired[0].Id()).To(Equal("test002"))

				records, err := btl.All()
				Expect(err).ToNot(HaveOccurred())
				Expect(records).To(HaveLen(1))
				Expect(records[0].Id()).To(Equal("test004"))
			})
		})
	})
})

var (
	baseTime = time.Unix(0, 0).UTC()
)

var mockFactory = func(data []byte) (record Record, err error) {

	idNum, err := strconv.Atoi(string(data))
	if err != nil {
		err = errors.Wrapf(err, "mockFactory only works with integers, got: %s", data)
		return
	}

	record = &RecordMock{
		EncodeFunc: func() ([]byte, error) {
			return data, nil
		},
		IdFunc: func() string {
			return fmt.Sprintf("test%s", data)
		},
		TsFunc: func() time.Time {
			return baseTime.Add(time.Duration(idNum) * time.Minute)
		},
	}
	return
}
