local stored_session = gurt.crumbs.get("gurtpay_session")
if not stored_session then
    gurt.location.goto("/login")
end

local session_data = JSON.parse(stored_session)
local current_user = session_data.user

do
    local uname_el = gurt.select("#username")
    if uname_el then
        uname_el.text = current_user.username
    end
end
