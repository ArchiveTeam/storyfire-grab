dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local discovered = {}

local bad_items = {}

if not urlparse or not http then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  if string.match(urlparse.unescape(url), "[<>\\]") then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if not tested[s] then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  for s in string.gmatch(url, "([0-9a-zA-Z]+)") do
    if ids[s] then
      return true
    end
  end

  if item_type == "video"
    and (string.match(url, "%.m3u8") or string.match(url, "%.ts$")) then
    return true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, "/" .. newurl))
    end
  end

  local function checkpagination(url, n)
    for i=0,n,10 do
      check(string.gsub(url, "skip=[0-9]+", "skip=" .. tostring(i)))
    end
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "%.ts$")
    and not string.match(url, "^https?://[^/]*amazonaws%.com") then
    html = read_file(file)
    if string.match(url, "/video%-details/") then
      check("https://storyfire.com/app/videoEpisodes/" .. item_value .. "?skip=10")
      check("https://storyfire.com/app/generic/video-comments/" .. item_value .. "?skip=0")
      check("https://storyfire.com/app/generic/video-detail/" .. item_value)
      json = string.match(html, '<script%s+id="__NEXT_DATA__"%s+type="application/json">({.+})</script>')
      if json then
        json = JSON:decode(json)
      end
      if not json or not json["props"] or not json["props"]["initialState"]
        or not json["props"]["initialState"]["video"] then
        io.stdout:write("Could not get base video info.\n")
        abort_item()
      end
      json = json["props"]["initialState"]["video"]
      if not json["episodes"] or not json["currentVideo"]
        or not json["currentVideo"]["tippedUsers"]
        or not json["currentVideo"]["likes"] then
        io.stdout:write("Could not find video lists.\n")
        abort_item()
      end
      io.stdout:flush()
      for _, d in pairs(json["episodes"]) do
        discovered["video:" .. d["_id"]] = true
      end
      for _, d in pairs(json["currentVideo"]["tippedUsers"]) do
        discovered["user:" .. d["tipped_by_user"]] = true
      end
      for _, d in pairs(json["currentVideo"]["likes"]) do
        discovered["user:" .. d] = true
      end
    end
    if string.match(url, "%?skip=0$") then
      check(string.match(url, "^([^%?]+)%?"))
    end
    if string.match(url, "^https?://storyfire%.com/app/publicVideos/.+?skip=[0-9]+$") then
      json = JSON:decode(html)
      if not json or not json["videos"] or not json["videosCount"] then
        io.stdout:write("Could not get videos information.\n")
        io.stdout:flush()
        abort_item()
      end
      for _, d in pairs(json["videos"]) do
        if not d["_id"] then
          io.stdout:write("Could not get video ID from JSON.\n")
          io.stdout:flush()
          abort_item()
        end
        discovered["video:" .. d["_id"]] = true
      end
      checkpagination(url, json["videosCount"])
    end
    if string.match(url, "^https?://storyfire%.com/app/videoEpisodes/.+?skip=[0-9]+$") then
      json = JSON:decode(html)
      if not json or not json["episodes"] or not json["episodesCount"] then
        io.stdout:write("Could not get episodes information.\n")
        io.stdout:flush()
        abort_item()
      end
      for _, d in pairs(json["episodes"]) do
        if not d["_id"] then
          io.stdout:write("Could not get video ID from JSON.\n")
          io.stdout:flush()
          abort_item()
        end
        discovered["video:" .. d["_id"]] = true
      end
      checkpagination(url, json["episodesCount"])
    end
    if string.match(url, "^https?://storyfire%.com/app/generic/video%-comments/.+?skip=[0-9]+$") then
      json = JSON:decode(html)
      if not json or not json["comment"] or not json["commentCount"] then
        io.stdout:write("Could not get comments information.\n")
        io.stdout:flush()
        abort_item()
      end
      for _, d in pairs(json["comment"]) do
        if not d["created_by"] or not d["created_by"]["_id"] then
          io.stdout:write("Could not get user ID from comments JSON.\n")
          io.stdout:flush()
          abort_item()
        end
        discovered["user:" .. d["created_by"]["_id"]] = true
      end
      checkpagination(url, json["commentCount"])
    end
    if string.match(url, "%.m3u8") then
      if not string.find(url, "playlist") then
        highest_bandwidth = 0
        highest_url = nil
        highest_audio = nil
        for line in string.gmatch(html, "([^\n]+)") do
          local bandwidth = string.match(line, "AVERAGE%-BANDWIDTH=([0-9]+)")
          local audio = string.match(line, 'AUDIO="audio%-([^"]+)"')
          if bandwidth then
            bandwidth = tonumber(bandwidth)
            if bandwidth > highest_bandwidth then
              highest_bandwidth = bandwidth
              highest_url = nil
              if highest_audio and highest_audio ~= audio then
                html = string.gsub(html, '[^\n]+GROUP%-ID="audio%-' .. highest_audio .. '"[^\n]+', "")
              end
              highest_audio = audio
            end
          end
          if string.match(line, "^[^#]") and not highest_url then
            highest_url = line
          end
        end
        if highest_url then
          check(urlparse.absolute(url, highest_url))
        end
      else
        for line in string.gmatch(html, "([^\n]+)") do
          if string.match(line, "^[^#]") then
            check(urlparse.absolute(url, line))
          end
        end
      end
    end
    if string.match(url, "^https?://storyfire%.com/user/") then
      check("https://storyfire.com/user/" .. item_value .. "/video")
      check("https://storyfire.com/app/publicSeries/" .. item_value)
      check("https://storyfire.com/app/users/getProfile/" .. item_value)
      check("https://storyfire.com/app/publicVideos/" .. item_value .. "?skip=0")
    end
    if string.match(url, "^https?://storyfire%.com/app/publicSeries/") then
      json = JSON:decode(html)
      if not json or not json["series"] then
        io.stdout:write("Could not get series information.\n")
      end
      for _, d in pairs(json["series"]) do
        discovered["video:" .. d["_id"]] = true
        discovered["video:" .. d["first_episode"]["_id"]] = true
        discovered["video:" .. d["last_episode"]["_id"]] = true
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  local t, match = string.match(url["url"], "^https?://storyfire%.com/(video%-details)/([0-9a-f]+)$")
  if not match then
    t, match = string.match(url["url"], "^https?://storyfire%.com/(user)/([0-9a-zA-Z]+)$")
  end
  if t and match and not ids[match] then
    abortgrab = false
    ids[match] = true
    item_value = match
    if t == "video-details" then
      item_type = "video"
    elseif t == "user" then
      item_type = "user"
    else
      io.stdout:write("Could not determine item type.\n")
      io.stdout:flush()
    end
    item_name = item_type .. ":" .. match
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] or addedtolist[newloc]
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0
    or (status_code > 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 1
    if not allowed(url["url"], nil) then
      maxtries = 3
    end
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-items.txt', 'w')
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  local items = nil
  for item, _ in pairs(discovered) do
    print('found item', item)
    if items == nil then
      items = item
    else
      items = items .. "\0" .. item
    end
  end
  if items ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/storyfire-99fphrai0r35o21/",
        items
      )
      if code == 200 or code == 409 then
        break
      end
      io.stdout:write("Could not queue new items.\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      abortgrab = true
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    abort_item()
  end
  return exit_status
end

