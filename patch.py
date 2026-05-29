import os
import re

BOT_DIR = "/home/bishal/karen/bot"

# 1. Add GofileToken to structs.go
structs_path = os.path.join(BOT_DIR, "utils/structs/structs.go")
with open(structs_path, "r") as f:
    content = f.read()

if "GofileToken" not in content:
    content = content.replace(
        "TelegramRequestTimeoutSeconds int `yaml:\"telegram-request-timeout-seconds\"`",
        "TelegramRequestTimeoutSeconds int `yaml:\"telegram-request-timeout-seconds\"`\n\tGofileToken                   string  `yaml:\"gofile-token\"`"
    )
    with open(structs_path, "w") as f:
        f.write(content)

# 2. Add gofile-token to config.yaml
config_path = os.path.join(BOT_DIR, "config.yaml")
with open(config_path, "r") as f:
    content = f.read()

if "gofile-token" not in content:
    with open(config_path, "a") as f:
        f.write("\ngofile-token: \"\"\n")

# 3. Patch telegram_bot.go
tb_path = os.path.join(BOT_DIR, "telegram_bot.go")
with open(tb_path, "r") as f:
    content = f.read()

# Add import
if 'apputils "main/utils"' not in content:
    content = content.replace(
        '"main/utils/ampapi"',
        'apputils "main/utils"\n\t"main/utils/ampapi"'
    )

# Replace sendDocumentFile
send_doc_start = content.find("func (b *TelegramBot) sendDocumentFile(")
send_doc_end = content.find("func (b *TelegramBot) sendDocumentByFileID(")
if send_doc_start != -1 and send_doc_end != -1:
    new_send_doc = """func (b *TelegramBot) sendDocumentFile(chatID int64, filePath string, displayName string, replyToID int, status *DownloadStatus, cacheKey string) error {
	if displayName == "" {
		displayName = filepath.Base(filePath)
	}
	_, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if status != nil {
		status.Update("Uploading ZIP to GoFile", 0, 0)
	}

	downloadUrl, err := apputils.UploadToGofile(filePath, Config.GofileToken)
	if err != nil {
		if status != nil {
			status.Update("Upload failed: "+err.Error(), 0, 0)
		}
		return fmt.Errorf("failed to upload to gofile: %w", err)
	}

	messageText := fmt.Sprintf("✅ Upload successful!\\n\\n📂 **%s**\\n\\n🔗 Download Link: %s", displayName, downloadUrl)
	return b.sendMessageWithReply(chatID, messageText, nil, replyToID)
}

"""
    content = content[:send_doc_start] + new_send_doc + content[send_doc_end:]


# Replace sendAudioFile
send_audio_start = content.find("func (b *TelegramBot) sendAudioFile(")
send_audio_end = content.find("func (b *TelegramBot) sendDocumentFile(")
if send_audio_start != -1 and send_audio_end != -1:
    new_send_audio = """func (b *TelegramBot) sendAudioFile(chatID int64, filePath string, replyToID int, status *DownloadStatus, format string) error {
	displayName := filepath.Base(filePath)
	_, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if status != nil {
		status.Update("Uploading Audio to GoFile", 0, 0)
	}

	downloadUrl, err := apputils.UploadToGofile(filePath, Config.GofileToken)
	if err != nil {
		if status != nil {
			status.Update("Upload failed: "+err.Error(), 0, 0)
		}
		return fmt.Errorf("failed to upload to gofile: %w", err)
	}

	messageText := fmt.Sprintf("✅ Upload successful!\\n\\n🎵 **%s**\\n\\n🔗 Download Link: %s", displayName, downloadUrl)
	return b.sendMessageWithReply(chatID, messageText, nil, replyToID)
}

"""
    content = content[:send_audio_start] + new_send_audio + content[send_audio_end:]


with open(tb_path, "w") as f:
    f.write(content)

print("Patch applied successfully.")
