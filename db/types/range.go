package types

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

// Range represents a PostgreSQL int8range mapped to [2]uint64 in Go.
// The range is inclusive of the start and exclusive of the end (i.e. [start, end))
type Range [2]uint64

// Value implements the driver.Valuer interface
func (r Range) Value() (driver.Value, error) {
	return fmt.Sprintf("[%d,%d)", r[0], r[1]), nil
}

// Scan implements the sql.Scanner interface
func (r *Range) Scan(src interface{}) error {
	var rangeStr string

	switch v := src.(type) {
	case string:
		rangeStr = v
	case []byte:
		rangeStr = string(v)
	default:
		return fmt.Errorf("unsupported range type: %T", src)
	}

	// Parse the PostgreSQL range format (e.g., "[10,20)")
	rangeStr = strings.Trim(rangeStr, "[)")
	parts := strings.Split(rangeStr, ",")
	if len(parts) != 2 {
		return fmt.Errorf("invalid range format: %s", rangeStr)
	}

	start, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing range start: %w", err)
	}

	end, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing range end: %w", err)
	}

	r[0] = start
	r[1] = end

	return nil
}
