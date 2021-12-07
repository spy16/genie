local inspect = require('inspect')
local genie = require('genie')

genie.Register("Print", function(item)
    print("foo")
end)

genie.Register("Log", function(item)
    inspect(item)
end)
