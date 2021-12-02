package lua_test

import (
	lua2 "github.com/spy16/genie/lua"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	called := false

	l, err := lua2.New(
		lua2.Globals(map[string]interface{}{
			"foo": func() {
				called = true
			},
		}),
	)
	assert.NoError(t, err)
	require.NotNil(t, l)
	defer l.Destroy()

	assert.False(t, called)
	err = l.Execute("foo()")
	assert.NoError(t, err)
	assert.True(t, called)
}
